//! Two-layer opt-in for upstream `terraform-provider-aws` acceptance tests.
//!
//! Layer 1: `SERVICES` is an allow-list. A service is only exercised at all
//! if it appears here. This matches fakecloud's parity-per-implemented-service
//! invariant — we don't want CI noise from services we don't claim to
//! support.
//!
//! Layer 2: each `Service` carries a `deny` array of specific upstream test
//! names to skip, with reasons grouped in inline comments. These are passed
//! to `go test -skip '^(name1|name2|...)$'` (Go ≥ 1.20).
//!
//! Deny-list semantics:
//!
//! * **unsupportable**: the test needs a fakecloud feature that we don't
//!   plan to implement (cross-region replicas, real backup encryption,
//!   import from S3). Stays denied permanently.
//! * **gap**: the test fails because of a real fakecloud bug. Denied
//!   temporarily — driving these to zero is the point of later batches.
//! * **hung**: the test never completed in our initial triage run. Denied
//!   until we can characterise it; may move to gap or unsupportable later.
//!
//! Every entry must have a reason comment. Adding an entry without one is
//! a review-blocking mistake.

pub struct Service {
    /// Directory name under `internal/service/` — e.g. `sqs`, `dynamodb`.
    pub name: &'static str,
    /// Go `-run` regex. Narrow this to carve out a subset of a service's
    /// upstream tests while the rest of that service's deny-list is being
    /// populated. Widening (or removing the override) is the mechanism for
    /// growing coverage in later batches.
    pub run_regex: &'static str,
    /// Upstream test function names to skip, one per line, grouped by
    /// reason in inline comments.
    pub deny: &'static [&'static str],
}

/// A CI matrix entry. Normally a shard is 1:1 with a `Service` (same
/// regex and deny-list) and the shard name equals the service name. For
/// the handful of services whose full test set exceeds a single runner's
/// wall-clock budget, we define multiple shards over the same service
/// with narrower run_regex + extended deny so the union of all shards
/// equals the service's original selection.
///
/// The shards are what `tfacc.yml` fans out over — one GitHub Actions
/// job per entry.
pub struct Shard {
    /// Matrix job name. For unsharded services this is just the service
    /// name; for sharded services it is `<service>-<suffix>`.
    pub name: &'static str,
    /// Upstream directory — passed to `go test ./internal/service/<svc>/`.
    pub service: &'static str,
    /// Narrower `-run` regex than the owning `Service` uses.
    pub run_regex: &'static str,
    /// Extra tests to `-skip` on top of the owning `Service`'s deny-list.
    /// Lets a sibling shard claim those tests without double-running them.
    pub extra_deny: &'static [&'static str],
}

/// Combine a shard's `extra_deny` with its owning service's `deny` list
/// and return the full set CI should skip.
pub fn shard_deny_list(shard: &Shard) -> Vec<&'static str> {
    let service = SERVICES
        .iter()
        .find(|s| s.name == shard.service)
        .expect("shard references unknown service");
    let mut out: Vec<&'static str> = service.deny.to_vec();
    out.extend(shard.extra_deny.iter().copied());
    out
}

pub const SERVICES: &[Service] = &[
    Service {
        name: "cognitoidp",
        // Batch 11: core `aws_cognito_user_pool` smoke. The fix here is
        // returning five shape blocks with AWS defaults on every
        // DescribeUserPool, which Terraform's provider asserts after
        // create: `email_configuration.email_sending_account =
        // COGNITO_DEFAULT`, `verification_message_template.
        // default_email_option = CONFIRM_WITH_CODE`,
        // `sign_in_policy.allowed_first_auth_factors = ["PASSWORD"]`,
        // `user_pool_tier = ESSENTIALS`, and a non-empty
        // `account_recovery_setting`. None of these were emitted
        // unless the caller set them at create time.
        run_regex: "^TestAccCognitoIDPUserPool_basic$",
        deny: &[],
    },
    Service {
        name: "bedrock",
        // Batch 10: `data.aws_bedrock_foundation_models` data source
        // smoke. fakecloud's Bedrock implementation already returns the
        // expected ListFoundationModels shape, so this passes out of
        // the box. Resource tests (model invocation, guardrails) need
        // the Bedrock runtime container path to be plumbed through TF
        // and are deferred to a later batch.
        run_regex: "^TestAccBedrockFoundationModelsDataSource_basic$",
        deny: &[],
    },
    Service {
        name: "apigatewayv2",
        // Batch 9: core `aws_apigatewayv2_api` (HTTP) smoke. The fix
        // here is making CreateApi return four metadata fields that
        // real AWS always populates (api_key_selection_expression,
        // route_selection_expression, disable_execute_api_endpoint,
        // ip_address_type) — Terraform's provider asserts on each of
        // them on every refresh.
        run_regex: "^TestAccAPIGatewayV2API_basicHTTP$",
        deny: &[],
    },
    Service {
        name: "kinesis",
        // Batch 8: core `aws_kinesis_stream` smoke. The fix here is
        // making `IncreaseStreamRetentionPeriod` accept same-value as a
        // no-op — real AWS does this despite what the API docs say,
        // and the upstream provider unconditionally calls it with the
        // default 24h on every create.
        run_regex: "^TestAccKinesisStream_basic$",
        deny: &[],
    },
    Service {
        name: "sns",
        // Batch 7: core `aws_sns_topic` smoke. Passes against fakecloud
        // out of the box.
        run_regex: "^TestAccSNSTopic_basic$",
        deny: &[],
    },
    Service {
        name: "events",
        // Batch 7: core EventBridge `aws_cloudwatch_event_bus` and
        // `aws_cloudwatch_event_rule` smokes. Both pass out of the box.
        // Note: the upstream service directory is `events`, not
        // `eventbridge` — Terraform uses the legacy CloudWatch Events
        // naming.
        run_regex: "^TestAccEvents(Bus|Rule)_basic$",
        deny: &[],
    },
    Service {
        name: "kms",
        // Batch 6: core `aws_kms_key` smoke. Passes against fakecloud
        // out of the box.
        run_regex: "^TestAccKMSKey_basic$",
        deny: &[],
    },
    Service {
        name: "logs",
        // Batch 6: core `aws_cloudwatch_log_group` smoke. The fix here
        // is making DescribeLogGroups always return `logGroupClass`
        // (defaulting to STANDARD), which Terraform's provider asserts
        // on every refresh.
        run_regex: "^TestAccLogsGroup_basic$",
        deny: &[],
    },
    Service {
        name: "iam",
        // Batch 5: core CRUD smoke for the four most-used IAM resource
        // types. Passes against fakecloud out of the box — no
        // fakecloud-side changes needed. Later batches widen to
        // attached-policy, group-membership, and instance-profile tests.
        run_regex: "^TestAccIAM(Role|User|Policy|Group)_basic$",
        deny: &[],
    },
    Service {
        name: "ssm",
        // Batch 4: core `aws_ssm_parameter` smoke. The fix here is making
        // `lookup_param` tolerate the `name:version` selector that real
        // AWS accepts on GetParameter / ListTagsForResource — without it
        // the upstream import-with-version step fails with
        // InvalidResourceId.
        run_regex: "^TestAccSSMParameter_basic$",
        deny: &[],
    },
    Service {
        name: "secretsmanager",
        // Batch 4: core `aws_secretsmanager_secret` smoke. Passes against
        // fakecloud out of the box — no fakecloud-side changes needed.
        run_regex: "^TestAccSecretsManagerSecret_basic$",
        deny: &[],
    },
    Service {
        name: "sqs",
        // SQS tests are curated via a positive regex rather than
        // `^TestAcc` + deny-list because CI runners (2-core Linux) are
        // dramatically slower than dev machines — running the full 66
        // TestAcc set exceeds the 90m CI timeout. Adding a new batch
        // widens this regex by one cluster at a time.
        //
        // Batch 2: JSON canonicalization fix — redrive + policy round trip.
        // Batch 3: encryption defaults + mode-switch reset.
        run_regex: concat!(
            "^TestAccSQS(",
            // core queue smoke + JSON-canonicalized attributes
            "Queue_(basic|redrivePolicy|redriveAllowPolicy|Policy_basic",
            // encryption attribute round-trip
            "|encryption|managedEncryption",
            "|defaultKMSDataKeyReusePeriodSeconds",
            "|ManagedEncryption_kmsDataKeyReusePeriodSeconds",
            "|noEncryptionKMSDataKeyReusePeriodSeconds)",
            // separate resources for policy and redrive subresources
            "|QueuePolicy_basic",
            "|QueueRedrivePolicy_basic",
            "|QueueRedriveAllowPolicy_basic)$",
        ),
        // Characterised: `TestAccSQSQueueRedriveAllowPolicy_basic` passes
        // against fakecloud. It is slow (~2m) only because the SQS provider's
        // `waitQueueAttributesPropagated` enforces `ContinuousTargetOccurence:
        // 6` at `MinTimeout: 5s` — a client-side stabilisation wait that is
        // backend-speed-independent. That cost is bounded and well under the
        // shard timeout, so the test no longer needs to be denied.
        deny: &[],
    },
    Service {
        name: "dynamodb",
        // Batch 1: only the `aws_dynamodb_table` resource tests. The
        // upstream dynamodb service directory also has ~90 tests covering
        // table_item, replica, export, kinesis_streaming_destination, and
        // global_table which surface deeper fakecloud gaps and will be
        // added in follow-up batches.
        run_regex: "^TestAccDynamoDBTable_",
        deny: &[
            // --- unsupportable: DynamoDB Global Tables / cross-region replicas ---
            "TestAccDynamoDBTable_Replica_single",
            "TestAccDynamoDBTable_Replica_singleCMK",
            "TestAccDynamoDBTable_Replica_singleDefaultKeyEncrypted",
            "TestAccDynamoDBTable_Replica_singleDefaultKeyEncryptedAmazonOwned",
            "TestAccDynamoDBTable_Replica_singleStreamSpecification",
            "TestAccDynamoDBTable_Replica_multiple",
            "TestAccDynamoDBTable_Replica_doubleAddCMK",
            "TestAccDynamoDBTable_Replica_pitr",
            "TestAccDynamoDBTable_Replica_pitrKMS",
            "TestAccDynamoDBTable_Replica_tagsUpdate",
            "TestAccDynamoDBTable_Replica_tags_propagateToAddedReplica",
            "TestAccDynamoDBTable_Replica_tags_notPropagatedToAddedReplica",
            "TestAccDynamoDBTable_Replica_tags_nonPropagatedTagsAreUnmanaged",
            "TestAccDynamoDBTable_Replica_tags_updateIsPropagated_oneOfTwo",
            "TestAccDynamoDBTable_Replica_tags_updateIsPropagated_twoOfTwo",
            "TestAccDynamoDBTable_restoreCrossRegion",
            // --- unsupportable: INFREQUENT_ACCESS table class ---
            "TestAccDynamoDBTable_tableClassInfrequentAccess",
            "TestAccDynamoDBTable_tableClass_migrate",
            "TestAccDynamoDBTable_tableClass_ConcurrentModification",
            // --- unsupportable: backup encryption (S3 import/export path) ---
            "TestAccDynamoDBTable_backupEncryption",
            "TestAccDynamoDBTable_backup_overrideEncryption",
            "TestAccDynamoDBTable_importTable",
            // --- unsupportable: the test sleeps 60m in a PreConfig because AWS
            //     refuses to disable TTL within an hour of enabling it
            //     (upstream issue #39195), so it can never finish inside CI
            //     regardless of fakecloud behavior. ---
            "TestAccDynamoDBTable_TTL_updateDisable",
        ],
    },
];

/// CI matrix shards. One GitHub Actions job per entry.
///
/// Kept as a flat list (rather than generated from `SERVICES`) so a reader
/// can see exactly which services are split and why. Services with one
/// shard here use the default service regex + deny. Services with
/// multiple shards partition the run_regex; `shard_deny_list` merges the
/// service's deny with each shard's extra_deny to keep sibling shards
/// from double-running the same tests.
pub const SHARDS: &[Shard] = &[
    // ─── unsharded services (1 shard each) ─────────────────────────
    Shard {
        name: "cognitoidp",
        service: "cognitoidp",
        run_regex: "^TestAccCognitoIDPUserPool_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "bedrock",
        service: "bedrock",
        run_regex: "^TestAccBedrockFoundationModelsDataSource_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "apigatewayv2",
        service: "apigatewayv2",
        run_regex: "^TestAccAPIGatewayV2API_basicHTTP$",
        extra_deny: &[],
    },
    Shard {
        name: "kinesis",
        service: "kinesis",
        run_regex: "^TestAccKinesisStream_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "sns",
        service: "sns",
        run_regex: "^TestAccSNSTopic_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "events",
        service: "events",
        run_regex: "^TestAccEvents(Bus|Rule)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "kms",
        service: "kms",
        run_regex: "^TestAccKMSKey_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "logs",
        service: "logs",
        run_regex: "^TestAccLogsGroup_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "iam",
        service: "iam",
        run_regex: "^TestAccIAM(Role|User|Policy|Group)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "ssm",
        service: "ssm",
        run_regex: "^TestAccSSMParameter_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "secretsmanager",
        service: "secretsmanager",
        run_regex: "^TestAccSecretsManagerSecret_basic$",
        extra_deny: &[],
    },
    // ─── sqs split into core + encryption ──────────────────────────
    // The full sqs regex is a union of ~12 TestAcc names; split so the
    // core queue/policy/redrive suite runs in parallel with the
    // encryption-attribute round-trip suite. Wall-clock for sqs drops
    // from ~7m to ~the slower half of that.
    Shard {
        name: "sqs-core",
        service: "sqs",
        run_regex: concat!(
            "^TestAccSQS(",
            "Queue_(basic|redrivePolicy|redriveAllowPolicy|Policy_basic)",
            "|QueuePolicy_basic",
            "|QueueRedrivePolicy_basic",
            "|QueueRedriveAllowPolicy_basic)$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "sqs-encryption",
        service: "sqs",
        run_regex: concat!(
            "^TestAccSQSQueue_(",
            "encryption|managedEncryption",
            "|defaultKMSDataKeyReusePeriodSeconds",
            "|ManagedEncryption_kmsDataKeyReusePeriodSeconds",
            "|noEncryptionKMSDataKeyReusePeriodSeconds)$",
        ),
        extra_deny: &[],
    },
    // ─── dynamodb split into a-g vs h-z ────────────────────────────
    // dynamodb's `^TestAccDynamoDBTable_` regex selects ~50 tests after
    // deny-listing. Splitting into two halves keyed on the first letter
    // after the underscore roughly halves the wall-clock of the longest
    // shard. Go test's -skip takes a regex, so we can cover the union
    // without enumerating every upstream test name.
    Shard {
        name: "dynamodb-a-g",
        service: "dynamodb",
        run_regex: "^TestAccDynamoDBTable_[a-gA-G]",
        extra_deny: &[],
    },
    Shard {
        name: "dynamodb-h-z",
        service: "dynamodb",
        run_regex: "^TestAccDynamoDBTable_[^a-gA-G]",
        extra_deny: &[],
    },
];
