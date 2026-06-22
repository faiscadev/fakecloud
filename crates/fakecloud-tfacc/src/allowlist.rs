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
        name: "s3",
        // Wave 3: the `_basic` suite for the S3 bucket sub-resources, bucket
        // and object data sources, and the object resources. 24 tests. The
        // batch made GetObject/HeadObject report the default SSE-S3 (AES256)
        // encryption and stop echoing a stored checksum unless the caller sets
        // `x-amz-checksum-mode: ENABLED`, which the object tests assert.
        run_regex: "^TestAccS3[A-Za-z]+_basic$",
        deny: &[
            // --- gap: bucket logging / inventory leave the destination bucket
            //     non-empty at destroy time (force-destroy / log-object
            //     cleanup) — CheckDestroy fails with BucketNotEmpty. ---
            "TestAccS3BucketLogging_basic",
            "TestAccS3BucketInventory_basic",
            // --- gap: replication configuration is cross-region. ---
            "TestAccS3BucketReplicationConfiguration_basic",
            // --- unsupportable: S3 Express One Zone directory buckets are a
            //     separate API surface (ListDirectoryBuckets, etc.) not
            //     implemented. ---
            "TestAccS3DirectoryBucket_basic",
            "TestAccS3DirectoryBucketsDataSource_basic",
        ],
    },
    Service {
        name: "sts",
        // Wave 3: the `aws_caller_identity` data source smoke. STS's other
        // surfaces (AssumeRole, GetSessionToken) are request actions, not
        // Terraform-managed resources.
        run_regex: "^TestAccSTS[A-Za-z]+_basic$",
        deny: &[],
    },
    Service {
        name: "route53",
        // Wave 3: the `_basic` suite for hosted zones, records (+ exclusive /
        // data sources), health checks, DNSSEC, query-logging, delegation sets,
        // traffic policies (+ instances), and CIDR locations — 14 tests. The
        // batch made Route 53 render its REST-XML errors in the `<ErrorResponse>`
        // wrapper (not S3's bare `<Error>`), so the AWS SDK can read the error
        // code; without it the provider's post-destroy `GetHostedZone` check saw
        // `UnknownError` and every zone-owning test failed at destroy.
        run_regex: "^TestAccRoute53[A-Za-z]+_basic$",
        deny: &[
            // --- gap: CIDR collection ARN must omit the account id
            //     (`arn:aws:route53:::cidrcollection/...`); fakecloud includes
            //     it. ---
            "TestAccRoute53CIDRCollection_basic",
            // --- gap: DNSSEC key-signing keys need a computed DS digest_value
            //     (hex), which fakecloud leaves empty. ---
            "TestAccRoute53KeySigningKey_basic",
            // --- gap: VPC zone association apply path. ---
            "TestAccRoute53ZoneAssociation_basic",
        ],
    },
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
        // Widen: `_basic` smoke for the Cognito user-pool resources and data
        // sources — user pool (+ data sources), user pool client (+ data
        // sources), resource server, users and groups (+ data sources), and the
        // signing-certificate data source. The widen batch added the user-pool
        // client defaults the provider asserts (AuthSessionValidity = 3,
        // RefreshTokenValidity = 30, EnablePropagateAdditionalUserContextData).
        run_regex: "^TestAccCognitoIDP[A-Za-z]+_basic$",
        deny: &[
            // --- gap: federated identity providers need real SAML/OIDC
            //     metadata handling and attribute-mapping round-trip. ---
            "TestAccCognitoIDPIdentityProvider_basic",
            // --- gap: a user-pool domain provisions a CloudFront distribution
            //     (cloudfront_distribution / _arn), which fakecloud's Cognito
            //     does not stand up. ---
            "TestAccCognitoIDPUserPoolDomain_basic",
            // --- gap: managed (AWS-provisioned) user-pool clients are created
            //     by other AWS services, not directly; not modelled. ---
            "TestAccCognitoIDPManagedUserPoolClient_basic",
        ],
    },
    Service {
        name: "bedrock",
        // Batch 10 + widen: the foundation-model data sources (single + list),
        // which return the expected ListFoundationModels / GetFoundationModel
        // shapes out of the box.
        run_regex: "^TestAccBedrock[A-Za-z]+_basic$",
        deny: &[
            // --- gap: guardrails need the content-policy / topic-policy
            //     evaluation engine, which fakecloud's Bedrock does not model. ---
            "TestAccBedrockGuardrail_basic",
            "TestAccBedrockGuardrailVersion_basic",
            // --- gap: inference profiles (cross-region model routing) are not
            //     modelled. ---
            "TestAccBedrockInferenceProfile_basic",
            "TestAccBedrockInferenceProfileDataSource_basic",
            "TestAccBedrockInferenceProfilesDataSource_basic",
        ],
    },
    Service {
        name: "apigatewayv2",
        // Batch 9 + widen: the HTTP-API smoke plus the `_basic` suite for the
        // API Gateway v2 resources and data sources — model, route, route
        // response, integration response, VPC link (+ data source), deployment,
        // and custom domain name. The widen batch reported deployments as
        // synchronously DEPLOYED, custom domains as AVAILABLE (with a regional
        // endpoint + ipv4), and gave integrations their default
        // `connection_type = INTERNET`, all of which the provider asserts.
        run_regex: "^TestAccAPIGatewayV2([A-Za-z]+_basic|API_basicHTTP)$",
        deny: &[
            // --- gap: a REQUEST authorizer wires up a real Lambda authorizer
            //     function, which fakecloud cannot stand up without a working
            //     Lambda runtime in this environment. ---
            "TestAccAPIGatewayV2Authorizer_basic",
            // --- gap: the export data source generates an OpenAPI document
            //     from the API; fakecloud's export does not yet round-trip
            //     cleanly against the provider's expectations. ---
            "TestAccAPIGatewayV2ExportDataSource_basic",
        ],
    },
    Service {
        name: "kinesis",
        // Batch 8 + widen: `_basic` smoke for the Kinesis stream resources and
        // data sources. The widen batch added EnhancedMonitoring to
        // DescribeStreamSummary (so the data source reads `shard_level_metrics`)
        // and made CreateStream persist its initial Tags.
        run_regex: "^TestAccKinesis[A-Za-z]+_basic$",
        deny: &[
            // --- gap: the data source asserts a closed-shard count produced by
            //     a split/merge sequence; fakecloud's shard-lineage accounting
            //     closes fewer shards than real Kinesis (4 expected, 2 closed),
            //     which needs fuller split/merge parent-shard tracking. ---
            "TestAccKinesisStreamDataSource_basic",
        ],
    },
    Service {
        name: "sns",
        // Batch 7 + widen: `_basic` smoke for every SNS resource and data
        // source — topic, topic policy, topic subscription, topic data source,
        // and the data-protection policy (which the widen batch fixed: AWS
        // accepts an empty DataProtectionPolicy to clear it, fakecloud used to
        // reject the empty body the provider sends on delete).
        run_regex: "^TestAccSNS[A-Za-z]+_basic$",
        deny: &[],
    },
    Service {
        name: "events",
        // Batch 7 + widen: `_basic` smoke for the EventBridge resources and
        // data sources — event bus (+ buses data source), rule, target,
        // permission, connection, API destination, archive, and more. The
        // widen batch fixed PutPermission's `*` principal (stored verbatim,
        // not as an account-root ARN) and added CreationTime/LastModifiedTime
        // to ListEventBuses. Note: the upstream service directory is `events`,
        // not `eventbridge` — Terraform uses the legacy CloudWatch Events name.
        run_regex: "^TestAccEvents[A-Za-z]+_basic$",
        deny: &[
            // --- gap: EventBridge global endpoints need multi-region event
            //     replication / failover, which fakecloud does not model. ---
            "TestAccEventsEndpoint_basic",
        ],
    },
    Service {
        name: "kms",
        // Batch 6 + widen: `_basic` smoke for every KMS resource and data
        // source — keys, aliases, grants, key policies, ciphertext (data key)
        // and its data source, public-key and secrets data sources, and
        // external keys. The widen batch fixed external keys: an `EXTERNAL`
        // origin key has no material yet, so it must come back disabled in
        // `PendingImport`, which the provider asserts.
        run_regex: "^TestAccKMS[A-Za-z]+_basic$",
        deny: &[
            // --- unsupportable: multi-region (replica) keys need cross-region
            //     key replication, which fakecloud's single-region KMS state
            //     does not model. ---
            "TestAccKMSReplicaKey_basic",
            "TestAccKMSReplicaExternalKey_basic",
        ],
    },
    Service {
        name: "logs",
        // Batch 6 + widen: `_basic` smoke for the core CloudWatch Logs
        // resources and data sources — log group (+ data sources), log stream,
        // destination (+ policy), resource policy, query definition, metric
        // filter, and subscription filter. The widen batch added the metric
        // filter `unit` field (DescribeMetricFilters defaults it to None) and
        // fixed the Lambda empty-Environment drift the subscription-filter test
        // surfaced via its Lambda destination.
        run_regex: "^TestAccLogs[A-Za-z]+_basic$",
        deny: &[
            // --- gap: log anomaly detection — needs the anomaly-detector
            //     engine fakecloud does not model. ---
            "TestAccLogsAnomalyDetector_basic",
            // --- gap: data-protection policies — needs the data-protection
            //     policy engine (audit/deidentify) fakecloud does not model. ---
            "TestAccLogsDataProtectionPolicy_basic",
            "TestAccLogsDataProtectionPolicyDocumentDataSource_basic",
            // --- gap: CloudWatch Logs v2 vended-log delivery (delivery
            //     sources/destinations) is not implemented. ---
            "TestAccLogsDeliveryDestination_basic",
            "TestAccLogsDeliveryDestinationPolicy_basic",
            // --- gap: field index policies are not implemented. ---
            "TestAccLogsIndexPolicy_basic",
        ],
    },
    Service {
        name: "iam",
        // Batch 5 + widen: the `_basic` smoke for every IAM resource and data
        // source. Covers ~42 resource types (access keys, instance profiles,
        // OIDC/SAML providers, server certificates, every policy-attachment and
        // group-membership variant, service-specific credentials, signing
        // certificates, virtual MFA devices, ...). The widen batch added the
        // fakecloud-side fixes that unblocked server certificates (trailing-PEM
        // newline), the STS global-endpoint token version in GetAccountSummary,
        // and ListServiceSpecificCredentials `<member>` framing.
        run_regex: "^TestAccIAM[A-Za-z]+_basic$",
        deny: &[
            // --- gap: SimulatePrincipalPolicy returns no per-statement
            //          MatchedStatements detail (source_policy_id /
            //          source_policy_type), which the data source asserts.
            //          Needs the policy simulator to track which statement
            //          produced each decision. ---
            "TestAccIAMPrincipalPolicySimulationDataSource_basic",
            // --- gap: the data source lists existing roles and asserts the
            //          account is non-empty. Real accounts always carry
            //          AWS-managed service-linked roles; fakecloud seeds none,
            //          so a fresh account lists zero. Seeding default SLRs would
            //          perturb other tests' exact role counts, so deferred. ---
            "TestAccIAMRolesDataSource_basic",
            // --- gap: CreateServiceLinkedRole derives the role name by naive
            //          capitalisation (AWSServiceRoleForinspector) instead of
            //          AWS's per-service canonical name
            //          (AWSServiceRoleForAmazonInspector). Needs a
            //          service-principal -> SLR-name mapping table. ---
            "TestAccIAMServiceLinkedRole_basic",
        ],
    },
    Service {
        name: "ssm",
        // Batch 4 + widen: the `aws_ssm_parameter` family (parameter, its data
        // sources, the ephemeral resource, the by-path data source) plus
        // resource data sync. These pass against fakecloud as-is. The regex is
        // an explicit positive list rather than `^TestAccSSM..._basic$`
        // because the broader SSM resources (document, association, the
        // maintenance-window family, patch baselines) panic the upstream test
        // binary mid-run today; they are left for a dedicated SSM batch rather
        // than denied one-by-one.
        run_regex: concat!(
            "^TestAccSSM(",
            "Parameter|ParameterDataSource|ParameterEphemeral",
            "|ParametersByPathDataSource|ResourceDataSync",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "secretsmanager",
        // Batch 4 + widen: `_basic` smoke for the Secrets Manager resources and
        // data sources — secret (+ data source), secret policy, secret version
        // data sources, random-password data source and ephemeral resource.
        // Passes against fakecloud out of the box.
        run_regex: "^TestAccSecretsManager[A-Za-z]+_basic$",
        deny: &[
            // --- gap: secret rotation drives a Lambda rotation function on a
            //     schedule, which fakecloud does not orchestrate. ---
            "TestAccSecretsManagerSecretRotation_basic",
            "TestAccSecretsManagerSecretRotationDataSource_basic",
        ],
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
    // s3 split three ways: the full `_basic` set runs ~24 tests, each of which
    // creates a bucket, applies a sub-resource config, and destroys it. On a
    // 2-core CI runner that exceeds the 60-minute job budget, so partition the
    // bucket sub-resources A-L / M-Z and run the object resources separately.
    Shard {
        name: "s3-buckets-a",
        service: "s3",
        run_regex: "^TestAccS3Bucket[A-L][A-Za-z]*_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "s3-buckets-b",
        service: "s3",
        run_regex: "^TestAccS3Bucket[M-Z][A-Za-z]*_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "s3-objects",
        service: "s3",
        run_regex: "^TestAccS3(Object|Canonical)[A-Za-z]*_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "sts",
        service: "sts",
        run_regex: "^TestAccSTS[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "route53",
        service: "route53",
        run_regex: "^TestAccRoute53[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "cognitoidp",
        service: "cognitoidp",
        run_regex: "^TestAccCognitoIDP[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "bedrock",
        service: "bedrock",
        run_regex: "^TestAccBedrock[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "apigatewayv2",
        service: "apigatewayv2",
        run_regex: "^TestAccAPIGatewayV2([A-Za-z]+_basic|API_basicHTTP)$",
        extra_deny: &[],
    },
    Shard {
        name: "kinesis",
        service: "kinesis",
        run_regex: "^TestAccKinesis[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "sns",
        service: "sns",
        run_regex: "^TestAccSNS[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "events",
        service: "events",
        run_regex: "^TestAccEvents[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "kms",
        service: "kms",
        run_regex: "^TestAccKMS[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "logs",
        service: "logs",
        run_regex: "^TestAccLogs[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "iam",
        service: "iam",
        run_regex: "^TestAccIAM[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "ssm",
        service: "ssm",
        run_regex: concat!(
            "^TestAccSSM(",
            "Parameter|ParameterDataSource|ParameterEphemeral",
            "|ParametersByPathDataSource|ResourceDataSync",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "secretsmanager",
        service: "secretsmanager",
        run_regex: "^TestAccSecretsManager[A-Za-z]+_basic$",
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
    // ─── dynamodb non-table resources ──────────────────────────────
    // The two table shards cover `aws_dynamodb_table` only. This shard adds
    // the other DynamoDB resources and data sources that pass: table item
    // (+ data source), the table data source, contributor insights, and
    // tagging. The regex is an explicit positive list — the remaining
    // non-table resources are deliberately omitted, not denied:
    //   * kinesis_streaming_destination — `approximate_creation_date_time
    //     _precision` round-trip,
    //   * resource_policy — import-state-verify attribute mismatch,
    //   * global_table / table_replica — cross-region replication,
    //   * table_export — S3 export path,
    // each of which needs a dedicated fix and will be added later.
    Shard {
        name: "dynamodb-resources",
        service: "dynamodb",
        run_regex: concat!(
            "^TestAccDynamoDB(",
            "ContributorInsights|Tag|TableItem|TableItemDataSource|TableDataSource",
            ")_basic$",
        ),
        extra_deny: &[],
    },
];
