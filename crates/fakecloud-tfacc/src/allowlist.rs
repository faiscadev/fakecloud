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
            // (Bucket logging / inventory no longer eagerly deliver access-log
            //  or report objects into the destination bucket — like real S3,
            //  which delivers them async/scheduled — so a create+destroy test
            //  leaves the bucket empty and CheckDestroy passes. Eager delivery
            //  is opt-in via `FAKECLOUD_S3_EAGER_DELIVERY`.)
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
            // (CIDR collection ARN now omits the account id —
            //  `arn:aws:route53:::cidrcollection/...` — so that test runs.)
            // (Batch 20: DNSSEC key-signing keys now report the full DNSSEC
            //  material — the CreateKeySigningKey/GetDNSSEC response carries the
            //  base64 PublicKey, the DNSKEY/DS presentation records, and the
            //  upper-case SHA-256 DigestValue, all derived deterministically from
            //  the (zone, ksk-name) keypair fakecloud already generates — so the
            //  resource's public_key/dnskey_record/ds_record/digest_value
            //  assertions pass.)
            // (Private-zone VPC association now works: a zone created with a VPC
            //  is private and carries default NS/SOA records, and
            //  ListHostedZonesByVPC returns a bare HostedZoneId.)
        ],
    },
    Service {
        name: "organizations",
        // Wave 3: the policies-for-target data source smoke. Most other
        // Organizations resources mutate the singleton org and are exercised
        // elsewhere.
        run_regex: "^TestAccOrganizations[A-Za-z]+_basic$",
        deny: &[],
    },
    Service {
        name: "ecr",
        // Wave 3: the `_basic` suite for repository/lifecycle/pull-through-cache
        // policies and their data sources, plus the authorization-token data
        // source — 8 tests.
        run_regex: "^TestAccECR[A-Za-z]+_basic$",
        deny: &[
            // --- unsupportable: fakecloud reports the repository_url as its
            //     local registry endpoint (127.0.0.1:port/...) so real
            //     `docker push`/`pull` work against it; that can't also equal
            //     the AWS-format `<account>.dkr.ecr.<region>.amazonaws.com` URL
            //     the resource asserts. ---
            "TestAccECRRepository_basic",
            // --- gap: the image data source needs an image actually pushed to
            //     the registry, which requires a running container engine. ---
            "TestAccECRImageDataSource_basic",
            // --- gap: repository creation templates are not implemented. ---
            "TestAccECRRepositoryCreationTemplate_basic",
            "TestAccECRRepositoryCreationTemplateDataSource_basic",
        ],
    },
    Service {
        name: "glue",
        // Wave 3: the `_basic` suite for the Data Catalog (table + data source),
        // connections (+ data source), registries (+ data source), data-quality
        // rulesets, security configurations, user-defined functions, and
        // workflows — 10 tests.
        // Batch 15 reclaimed schema registry schemas, ML transforms, and dev
        // endpoints: GetSchema resolves a schema by ARN (parsing both the
        // registry and schema name out of the `schema/<reg>/<name>` path); ML
        // transforms carry a `tfm-` id prefix and report the `Schema` computed
        // from their input table's columns; dev endpoints settle to READY at
        // once (fakecloud has no notebook env to provision) and default to 5
        // nodes when no worker type is given.
        run_regex: "^TestAccGlue[A-Za-z]+_basic$",
        deny: &[
            // (Jobs and triggers reference the AWS-managed IAM policy
            //  `AWSGlueServiceRole`; fakecloud now resolves it from the
            //  AWS-managed policy catalogue (#1880), so those tests run.)
            // (Partitions now return `catalog_id`, the full StorageDescriptor
            //  round-trips (bucket/sort/skewed columns), and
            //  GetPartitionIndexes raises EntityNotFound on a deleted table,
            //  so the partition + partition-index tests run.)
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
        // Batch 14 added federated identity providers (social providers get the
        // default `username` attribute mapping Cognito injects — `sub` for
        // Google/Apple, `id` for Facebook, `user_id` for Login with Amazon) and
        // user-pool domains (DescribeUserPoolDomain now reports the fronting
        // CloudFront distribution domain and the managed S3 assets bucket, which
        // the resource surfaces as cloudfront_distribution/_arn and s3_bucket).
        run_regex: "^TestAccCognitoIDP[A-Za-z]+_basic$",
        deny: &[
            // --- unsupportable: a managed user-pool client is auto-created by a
            //     companion AWS service; this test stands up an
            //     `aws_opensearch_domain` (named `AmazonOpenSearchService-...`)
            //     to provision it, and fakecloud does not implement OpenSearch. ---
            "TestAccCognitoIDPManagedUserPoolClient_basic",
        ],
    },
    Service {
        name: "bedrock",
        // Batch 10 + widen: the foundation-model data sources (single + list),
        // which return the expected ListFoundationModels / GetFoundationModel
        // shapes out of the box.
        // Batch 16 reclaimed guardrails and inference profiles. GetGuardrail now
        // renders the stored policies in their read shape (dropping the `Config`
        // suffix from the wrapper keys, e.g. `filtersConfig` -> `filters`) and
        // round-trips the contextual-grounding policy, so a guardrail refreshes
        // cleanly; guardrail ops accept the ARN as the identifier so
        // CreateGuardrailVersion resolves. Inference profiles report status
        // `ACTIVE` and an `application-inference-profile/<id>` ARN, and the
        // AWS-managed SYSTEM_DEFINED cross-region catalogue is listed/resolvable
        // so the data sources read it.
        run_regex: "^TestAccBedrock[A-Za-z]+_basic$",
        deny: &[],
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
        // Batch 13 also restored the integration's `integration_method`,
        // `integration_response_selection_expression`, and `passthrough_behavior`
        // fields (the last two with their WEBSOCKET defaults), and gave
        // integrations their protocol-specific default timeout (29000ms for
        // WEBSOCKET, 30000ms for HTTP) so the export data source — which renders
        // the full integration into its OpenAPI document — round-trips cleanly.
        // Batch 19 reclaimed the REQUEST (Lambda) authorizer: fakecloud stands
        // up the authorizer Lambda via the container runtime (the same path the
        // Lambda Function tfacc tests use), and CreateAuthorizer/UpdateAuthorizer
        // now round-trip enable_simple_responses, authorizer_payload_format_version,
        // and authorizer_result_ttl_in_seconds so the update step does not drift.
        run_regex:
            "^TestAccAPIGatewayV2([A-Za-z]+_basic|API_basicHTTP|Integration_basic(HTTP|WebSocket))$",
        deny: &[],
    },
    Service {
        name: "kinesis",
        // Batch 8 + widen: `_basic` smoke for the Kinesis stream resources and
        // data sources. The widen batch added EnhancedMonitoring to
        // DescribeStreamSummary (so the data source reads `shard_level_metrics`)
        // and made CreateStream persist its initial Tags.
        run_regex: "^TestAccKinesis[A-Za-z]+_basic$",
        // (UpdateShardCount now reshards through the shards' common refinement
        //  — split-all-then-merge — matching AWS's uniform-scaling lineage, so
        //  scaling 2 -> 3 leaves the 4 closed shards the data source asserts.)
        deny: &[],
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
        // Batch 17 reclaimed anomaly detectors (ListTagsForResource now resolves
        // their ARNs), data-protection policies (the put/get echo the supplied
        // logGroupIdentifier verbatim so log_group_name round-trips), and the
        // vended-log delivery destinations (DescribeDeliveryDestination reports
        // the destination type — derived from the destination resource ARN — and
        // ListTagsForResource resolves delivery-destination ARNs).
        run_regex: "^TestAccLogs[A-Za-z]+_basic$",
        deny: &[
            // --- gap: this data source's document GENERATION round-trips, and
            //     the Firehose extended_s3 default-options drift is now fixed, but
            //     its config still drifts on the cross-service `LogDeliveryEnabled`
            //     auto-tag: when a CloudWatch Logs data-protection policy names a
            //     Firehose stream as a findings destination, AWS tags that stream
            //     `LogDeliveryEnabled=true`. fakecloud does not perform that
            //     logs -> firehose cross-service tag write. ---
            "TestAccLogsDataProtectionPolicyDocumentDataSource_basic",
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
        // Batch 21: Simulate{Custom,Principal}Policy now attribute their
        // MatchedStatements to the source policy. Each resolved identity policy
        // is tracked with its simulation source id (a managed policy by its
        // name, an inline policy as `<kind>_<principal>_<policy>`), and the
        // evaluator exposes which statements matched the action/resource for the
        // decision, so the data source reads source_policy_id/source_policy_type.
        run_regex: "^TestAccIAM[A-Za-z]+_basic$",
        deny: &[
            // (RolesDataSource now passes: accounts seed the default
            //  AWSServiceRoleForSupport / TrustedAdvisor service-linked roles
            //  like real AWS, so a fresh ListRoles is non-empty.)
            // (ServiceLinkedRole now passes: CreateServiceLinkedRole maps the
            //  service principal to AWS's canonical SLR name via a documented
            //  table — e.g. inspector -> AWSServiceRoleForAmazonInspector.)
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
        // Batch 18 enabled secret rotation (resource + data source): RotateSecret
        // is fully implemented — it persists the rotation lambda ARN + rules,
        // marks rotation enabled, and actually invokes the rotation Lambda (via
        // the container runtime, the same path the Lambda Function tfacc tests
        // use) so RotateImmediately completes and the secret reports its
        // rotation configuration.
        run_regex: "^TestAccSecretsManager[A-Za-z]+_basic$",
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
            // (INFREQUENT_ACCESS / STANDARD table class round-trips on
            //  create + UpdateTable now, so tableClassInfrequentAccess,
            //  tableClassExplicitDefault, and tableClass_ConcurrentModification
            //  run.)
            // --- unsupportable: the migrate test pins the external
            //     `hashicorp/aws@4.57.0` provider for step 1, which predates
            //     the `AWS_ENDPOINT_URL_*` SDK env vars, so it cannot be
            //     redirected to fakecloud and talks to real AWS. ---
            "TestAccDynamoDBTable_tableClass_migrate",
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
    // ─── Wave 3 tail: services added with a narrow positive regex over their
    //     currently-passing resources/data sources. The broader suites for
    //     these need fixes (or a container engine) and are deferred rather than
    //     enumerated as denies. ──────────────────────────────────────────────
    Service {
        name: "cloudformation",
        run_regex: "^TestAccCloudFormation(ExportDataSource|Stack)_basic$",
        deny: &[],
    },
    Service {
        name: "cognitoidentity",
        run_regex: concat!(
            "^TestAccCognitoIdentity(",
            "Pool|PoolDataSource|PoolRolesAttachment",
            "|OpenIDTokenForDeveloperIdentityEphemeral",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "lambda",
        // Control-plane metadata resources/data sources that don't need a
        // container runtime: function URL, provisioned concurrency, layer
        // versions (+ data source, now returning LayerArn on read), aliases
        // (response is rendered in the AWS wire shape and routing config clears
        // on removal), the function data source ($LATEST-qualified arn lineage
        // via ListVersionsByFunction), event-invoke config (optional
        // MaximumEventAgeInSeconds), and now the core function resource +
        // permission + functions data source. The function VPC scaffold rides
        // on the EC2 VPC/subnet IPv6 generation, the security-group all-traffic
        // port representation, and a default LoggingConfig. RuntimeManagement
        // config is deferred (its CheckDestroy reads an unset id attribute);
        // code-signing needs the AWS Signer service.
        run_regex: concat!(
            "^TestAccLambda(",
            "FunctionURL|FunctionURLDataSource",
            "|LayerVersion|LayerVersionDataSource|ProvisionedConcurrencyConfig",
            "|Alias|FunctionDataSource|FunctionEventInvokeConfig",
            "|Function|Permission|FunctionsDataSource|RuntimeManagementConfig",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "ecs",
        run_regex: concat!(
            "^TestAccECS(",
            "ClusterCapacityProviders|ClustersDataSource",
            "|TaskDefinition|TaskExecutionDataSource",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "ec2",
        // Elastic IPs, key pairs, and the core VPC control plane. CreateVpc now
        // provisions the default SG / default NACL / main route table (so
        // `aws_vpc` reads back the four default-resource ids) and honors
        // `AmazonProvidedIpv6CidrBlock` (a generated /56, also via
        // AssociateVpcCidrBlock); subnets report `private_dns_name_options`;
        // ENIs derive `private_dns_name` and default to the VPC's `default`
        // security group. The remaining EC2 surface is brought in incrementally.
        run_regex: concat!(
            "^TestAcc(",
            "EC2(EIP|EIPsDataSource|KeyPair)",
            "|VPC(|Subnet|SecurityGroup|RouteTable|InternetGateway|NetworkACL",
            "|NetworkInterface|DHCPOptions|EgressOnlyInternetGateway",
            "|DefaultSecurityGroup|DefaultNetworkACL|Route",
            "|MainRouteTableAssociation|NetworkACLAssociation)",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "redshift",
        // Amazon Redshift is a full control plane (no real warehouse container
        // is stood up). The Terraform provider's `_basic` suite exercises the
        // whole surface: clusters (which transition creating -> available on
        // describe so the create waiter completes, and report a synthetic
        // Endpoint/leader node/public key/MultiAZ status), parameter groups
        // (Source=user round-trips so there's no perpetual drift), subnet
        // groups (+ data source), snapshots (with a real SnapshotArn), snapshot
        // copy grants/schedules (+ cluster association), HSM client certs and
        // configurations, event subscriptions, usage limits, per-cluster
        // logging (LogExports round-trip), cross-region snapshot-copy config,
        // authentication profiles, IAM-role attach, endpoint access (with a
        // synthesized interface VPC endpoint), the cluster + credentials data
        // sources, and Partner registration.
        run_regex: "^TestAccRedshift[A-Za-z]+_basic$",
        deny: &[
            // --- unsupportable: these resources create a Redshift *Serverless*
            //     namespace first (redshift-serverless CreateNamespace), a
            //     separate AWS service that fakecloud does not implement, so the
            //     apply fails before any Redshift call is made. Data sharing and
            //     zero-ETL integrations are built on that serverless surface. ---
            "TestAccRedshiftDataShareAuthorization_basic",
            "TestAccRedshiftDataShareConsumerAssociation_basic",
            "TestAccRedshiftDataSharesDataSource_basic",
            "TestAccRedshiftProducerDataSharesDataSource_basic",
            "TestAccRedshiftIntegration_basic",
        ],
    },
    Service {
        name: "rds",
        // RDS control-plane resources that need no DB engine container. The DB
        // subnet group reports `supported_network_types = [IPV4]` and a
        // `Complete` status. DB/cluster parameter groups and option groups now
        // round-trip correctly and run in the `rds-param-groups` /
        // `rds-option-groups` shards below. DB instances/clusters need Docker
        // and stay out.
        run_regex: "^TestAccRDSSubnetGroup_basic$",
        deny: &[],
    },
    Service {
        name: "elasticache",
        // Control-plane resources that don't need a cache container: users,
        // user groups (+ association), and subnet groups (+ data sources).
        // CreateUser/CreateUserGroup accept the case-insensitive "REDIS"
        // engine; subnet-group names are lowercased like AWS and round-trip
        // their tags; ModifyUserGroup settles back to `active` so the
        // provider's update waiter completes.
        run_regex: concat!(
            "^TestAccElastiCache(",
            "User|UserDataSource",
            "|UserGroup|UserGroupAssociation",
            "|SubnetGroup|SubnetGroupDataSource",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "elasticbeanstalk",
        // Elastic Beanstalk is a full control plane. The provider's
        // application and configuration-template resources are pure
        // control-plane CRUD; the environment resource creates an
        // environment that our async settle drives to `Ready`/`Green` fast
        // enough for Terraform's create waiter, and exposes the synthesized
        // web-tier resources (Auto Scaling group, launch configuration,
        // instance, load balancer, ELB-shaped endpoint) the provider asserts.
        //
        // The application-version acceptance test is NOT an Elastic Beanstalk
        // gap: it fails in its S3 fixture step, not in Elastic Beanstalk. The
        // upstream config uploads the source bundle to a dotted S3 bucket
        // (`tftest.applicationversion.bucket-<rand>`), which the AWS SDK
        // addresses virtual-hosted-style (`<bucket>.<endpoint>`) against the
        // loopback fakecloud endpoint — a hostname that cannot resolve, so the
        // `aws_s3_bucket` dependency never applies. That is an S3
        // virtual-hosted-addressing constraint of the loopback tfacc harness,
        // independent of Elastic Beanstalk; `CreateApplicationVersion` itself
        // is exercised by the conformance suite and the crate's unit tests.
        run_regex: concat!(
            "^TestAccElasticBeanstalk(",
            "Application_basic",
            "|ConfigurationTemplate_basic",
            "|Environment_basic",
            ")$",
        ),
        deny: &[],
    },
    Service {
        name: "memorydb",
        // MemoryDB is a full control plane (no Redis/Valkey data-plane container),
        // so the Terraform provider's control-plane resources are exercisable:
        // ACLs, users, parameter groups (a default group per family is seeded so
        // the provider's default-group read succeeds), snapshots, and clusters
        // (which transition creating -> available on describe, so the create
        // waiter completes; shard slots are partitioned to match).
        //
        // SubnetGroup_basic is excluded: its check asserts `vpc_id` equals the
        // real EC2 VPC the test created, which needs cross-service subnet ->
        // VPC resolution the memorydb crate does not have (the sibling
        // ElastiCache subnet group has the same limitation). Tracked follow-up.
        run_regex: concat!(
            "^TestAccMemoryDB(",
            "ACL|User|ParameterGroup|Snapshot|Cluster",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "servicediscovery",
        // AWS Cloud Map is a full control plane (no real Route 53 zone / DNS
        // data plane is stood up). The Terraform provider's namespace resources
        // (HTTP, public DNS, private DNS) plus their data sources are
        // exercisable, along with the synchronous Service resource + data source
        // and asynchronous instance registration. Create/update/delete calls
        // mint an Operation that settles to SUCCESS on the provider's first
        // GetOperation poll, so the create/delete waiters complete. Private DNS
        // namespaces are VPC-scoped and use the real EC2 VPC the test creates.
        run_regex: "^TestAccServiceDiscovery",
        deny: &[],
    },
    Service {
        name: "account",
        // AWS Account Management is a full control plane. The Terraform provider's
        // account resources operate on the caller's own account: alternate
        // contacts (get/put/delete), the primary contact information, and Region
        // opt-in status (aws_account_region). Each maps directly to the restJson1
        // control plane and its data sources round-trip.
        run_regex: "^TestAccAccount_serial$",
        deny: &[],
    },
    Service {
        name: "eks",
        // EKS is a full control plane (no real Kubernetes API server is spawned),
        // so the Terraform provider's control-plane cluster resources and data
        // sources are exercisable: clusters (which transition CREATING -> ACTIVE
        // on describe so the create waiter completes), access entries and their
        // access-policy associations, OIDC identity-provider configs, and Pod
        // Identity associations. Managed node groups, Fargate profiles, and
        // add-ons are deferred: they model real compute/networking a control
        // plane alone can't stand up faithfully.
        run_regex: concat!(
            "^TestAccEKS(",
            "Cluster|ClusterDataSource|ClustersDataSource|ClusterVersionsDataSource",
            "|AccessEntry|AccessEntryDataSource|AccessPolicyAssociation",
            "|IdentityProviderConfig|PodIdentityAssociation",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "efs",
        // Amazon EFS is a real, persisted control plane. The provider's core
        // resources round-trip through create/read/update/delete: a file system
        // (settling `creating` -> `available` on the next describe so the create
        // waiter completes, reporting its size breakdown, performance/throughput
        // modes, and replication-overwrite protection), a mount target (its
        // Availability Zone, VPC, network interface, and IP resolved from the
        // real Terraform-created subnet in EC2 state), an access point (POSIX
        // user + root directory), and the backup-policy and file-system-policy
        // sub-resources (which persist and read back).
        //
        // The replication-configuration resource is deferred: it stands up a
        // destination file system in a second Region via an aliased provider,
        // modelling a cross-Region data-movement lifecycle (and an AWS-managed
        // destination file system) a single-endpoint control plane can't
        // reproduce faithfully. The mount-target / access-point data sources are
        // deferred alongside their read-only assertions on that deep networking.
        run_regex: concat!(
            "^TestAccEFS(",
            "FileSystem|MountTarget|AccessPoint|BackupPolicy|FileSystemPolicy",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "mq",
        // Amazon MQ is a real, persisted control plane. The provider's core
        // resources round-trip through create/read/update/delete: a broker
        // (settling `CREATION_IN_PROGRESS` -> `RUNNING` on the next describe so
        // the create waiter completes, reporting its per-engine wire endpoints,
        // console URLs, IP addresses, deployment mode, and current
        // configuration) and a configuration (base64 `Data` revisions with an
        // engine type + authentication strategy that persist and read back).
        // The broker's inline `user` blocks and ARN-keyed tags round-trip too.
        run_regex: "^TestAccMQ(Broker|Configuration)_basic$",
        deny: &[],
    },
    Service {
        name: "glacier",
        // Amazon S3 Glacier stores real archive bytes and computes a real
        // SHA-256 tree hash, so the standalone vault resource round-trips
        // through create/describe/delete, its notification, access-policy, and
        // tag sub-resources persist and read back, and the vault lock settles
        // the InProgress state machine (create with complete_lock=false, read
        // via GetVaultLock, abort on destroy). completeLock permanently locks a
        // vault (destroy can't abort it) and disappears/ignoreEquivalent model
        // policy-equivalence + drift a control plane alone doesn't reproduce, so
        // they are deferred.
        run_regex:
            "^TestAccGlacier(Vault_basic|Vault_notification|Vault_policy|Vault_tags|VaultLock_basic)$",
        deny: &[],
    },
    Service {
        name: "backup",
        // AWS Backup is a control-plane mock (LocalStack Community treats it the
        // same): no real backup engine runs. Vaults and plans are the standalone
        // resources — a vault persists and describes; a plan round-trips through
        // create/get/delete; a selection attaches to a plan; audit frameworks and
        // report plans persist and settle to a COMPLETED deployment status on
        // describe so the create waiters complete. Jobs / recovery points model
        // data movement a control plane alone can't perform and are deferred.
        run_regex: "^TestAccBackup(Vault|Plan|Selection|Framework|ReportPlan)_basic$",
        deny: &[],
    },
    Service {
        name: "ram",
        // AWS Resource Access Manager is a control-plane service: a resource
        // share round-trips through create/read/update/delete (settling ACTIVE
        // immediately), its tags persist, and principal / resource associations
        // settle straight to ASSOCIATED so the create waiters complete. The
        // cross-account accepter flow (aws_ram_resource_share_accepter) and
        // aws_ram_sharing_with_organization need a second account / a real
        // organization a single-account acceptance run can't stand up, and
        // customer-managed permission versioning has no standalone resource, so
        // those are deferred.
        run_regex: "^TestAccRAM(ResourceShare_basic|ResourceShare_tags|PrincipalAssociation_basic|ResourceAssociation_basic)$",
        deny: &[],
    },
    Service {
        name: "ce",
        // AWS Cost Explorer is a control-plane service: anomaly monitors +
        // subscriptions and cost category definitions round-trip through
        // create/read/update/delete, and cost-allocation tag status persists.
        // The cost/usage analytics data sources return zeroed result sets (an
        // emulator has no billed spend), so tests that assert on real cost
        // figures are out of scope; only the config-resource smokes run.
        run_regex: "^TestAccCE(AnomalyMonitor_basic|AnomalySubscription_basic|CostCategory_basic|CostAllocationTag_basic)$",
        deny: &[],
    },
    Service {
        name: "s3tables",
        // Amazon S3 Tables is a control-plane service: table buckets,
        // namespaces, and tables round-trip through create/read/delete (tables
        // carry a real metadata-location pointer + version token), and the
        // bucket/table policy sub-resources persist. The Iceberg data plane
        // (real table maintenance / compaction / record expiration) runs no
        // engine, so tests that assert on materialized table data are out of
        // scope; the resource smokes run.
        run_regex: "^TestAccS3Tables(TableBucket|Namespace|Table|TableBucketPolicy|TablePolicy)_basic$",
        deny: &[],
    },
    Service {
        name: "lakeformation",
        // AWS Lake Formation is a governance control plane over Glue. Three
        // families round-trip fully against the mock and run here: the
        // registered-resource lifecycle (`aws_lakeformation_resource`:
        // RegisterResource / DescribeResource / DeregisterResource over an S3
        // location + IAM role), data-lake settings (`PutDataLakeSettings` /
        // `GetDataLakeSettings` verbatim round-trip incl. admins + default
        // permissions), and LF-tags (`aws_lakeformation_lf_tag`: Create / Get /
        // Update / Delete + Terraform import). Most other provider tests are
        // subtests of `TestAccLakeFormation_serial` that depend on a real Glue
        // catalogue (databases/tables) and fine-grained tag/permission
        // propagation across an actual query engine, which the mock does not
        // model (permissions / data-cells-filter over Glue tables, opt-ins on
        // real resources), so they are deferred.
        run_regex: "^TestAccLakeFormation(Resource_basic|Resource_hybridAccessEnabled|_serial)$/^(DataLakeSettings|LFTags)$/^basic$",
        deny: &[],
    },
    Service {
        name: "elasticsearch",
        // Amazon Elasticsearch Service (legacy) is a control-plane mock: a
        // domain persists and settles to Processing=false / Created=true on
        // describe so the create waiter completes, and delete removes it so the
        // destroy waiter sees ResourceNotFound. Only the basic domain resource
        // is exercised; VPC/Cognito/SAML-wired variants model networking a
        // control plane alone can't stand up.
        run_regex: "^TestAccElasticsearchDomain_basic$",
        deny: &[],
    },
    Service {
        name: "opensearch",
        // Amazon OpenSearch Service shares the same control-plane domain store
        // as Elasticsearch Service (one `es` scope). The basic domain resource
        // round-trips through the create/describe/delete waiters.
        run_regex: "^TestAccOpenSearchDomain_basic$",
        deny: &[],
    },
    Service {
        name: "cloudfront",
        // Cache/origin-request/realtime-log/log-delivery data sources. The
        // CloudFront resources (distribution, function, OAC, ...) are deferred.
        run_regex: concat!(
            "^TestAccCloudFront(",
            "CachePolicyDataSource|OriginRequestPolicyDataSource",
            "|RealtimeLogConfigDataSource|LogDeliveryCanonicalUserIDDataSource",
            ")_basic$",
        ),
        deny: &[],
    },
    Service {
        name: "sfn",
        // State-machine data source plus the Activity resource + data source.
        // Activities now resolve in the tag path (`ListTagsForResource`
        // accepts activity ARNs, not just state-machine ARNs).
        run_regex: "^TestAccSFN(StateMachineDataSource|Activity|ActivityDataSource)_basic$",
        deny: &[],
    },
    Service {
        name: "scheduler",
        // EventBridge Scheduler: the `_basic` smoke for schedules and schedule
        // groups. A target's RetryPolicy is always reported with AWS's defaults
        // when omitted (MaximumEventAgeInSeconds=86400, MaximumRetryAttempts=185),
        // which the resource reads unconditionally.
        run_regex: "^TestAccScheduler[A-Za-z0-9]+_basic$",
        deny: &[],
    },
    Service {
        name: "pipes",
        // EventBridge Pipes: the control-plane smokes for aws_pipes_pipe — basic
        // SQS source->target, disappears, description, desired_state, role ARN,
        // generated/prefixed names, target update, source filter criteria, and
        // the target InputTemplate transform. Real-only surfaces (Kafka/MSK/MQ
        // sources, Redshift/SageMaker/Batch/ECS targets, KMS, CloudWatch-Logs
        // log configuration) are left out of the smoke.
        run_regex: "^TestAccPipesPipe_(basicSQS|disappears|description|desiredState|roleARN|nameGenerated|namePrefix|tags|targetUpdate|sourceParameters_filterCriteria|targetParameters_inputTemplate)$",
        deny: &[],
    },
    Service {
        name: "sesv2",
        // SES v2: the `_basic` smoke for configuration sets (+ event destination
        // + data source), dedicated IP pools (+ data source), and email identity
        // (+ feedback / mail-from attributes, policy, data sources). Fixes:
        // GetConfigurationSet only reports DeliveryOptions/TrackingOptions when
        // configured (an empty default block forced a perpetual plan diff) and
        // round-trips max_delivery_seconds; ListTagsForResource resolves
        // dedicated-ip-pool ARNs and pools persist create-time tags;
        // PutEmailIdentityFeedbackAttributes treats an absent (false) bool as
        // "disable forwarding".
        run_regex: "^TestAccSESV2[A-Za-z0-9]+_basic$",
        deny: &[],
    },
    Service {
        name: "wafv2",
        // WAF v2: the `_basic` smoke for IP sets, regex pattern sets, rule groups,
        // and web ACLs (+ their data sources / association). Fixes: REGIONAL-scope
        // ARNs use the literal `regional` resource-path prefix (was the region
        // name), and GetWebACL only reports ApplicationIntegrationURL when the ACL
        // declares token domains (the provider always sends a default CaptchaConfig
        // even for a basic ACL, so the CAPTCHA endpoint must gate on token domains).
        // (WebACLLoggingConfiguration now passes: it provisions a Firehose
        //  delivery stream as the log destination, and Firehose's ExtendedS3
        //  description now reports the default option blocks AWS returns
        //  (CloudWatchLoggingOptions/CustomTimeZone/S3BackupMode), so the stream
        //  no longer re-plans.)
        run_regex: "^TestAccWAFV2[A-Za-z0-9]+_basic$",
        deny: &[],
    },
    Service {
        name: "firehose",
        // Kinesis Data Firehose: the `_basic` smoke for the extended-S3 delivery
        // stream (+ data source). DescribeDeliveryStream now reports the
        // ExtendedS3 defaults AWS always returns -- the CloudWatchLoggingOptions
        // block (Enabled=false), custom_time_zone (UTC), and s3_backup_mode
        // (Disabled) -- which the resource reads unconditionally.
        run_regex: "^TestAccFirehoseDeliveryStream(_basic|DataSource_basic)$",
        deny: &[],
    },
    Service {
        name: "cloudwatch",
        // CloudWatch: the `_basic` smoke for composite alarms, dashboards, metric
        // alarms, metric streams, and contributor-insight rules. Fix:
        // DeleteDashboards returns an (empty) DeleteDashboardsResult element so
        // the SDK can deserialize the response.
        run_regex: "^TestAccCloudWatch[A-Za-z0-9]+_basic$",
        deny: &[
            // --- gap: managed contributor-insight rules are AWS-vendor-published
            //     rule templates (per service namespace); fakecloud does not
            //     model the managed-rule catalogue the resource + data source
            //     enumerate. ---
            "TestAccCloudWatchContributorManagedInsightRule_basic",
            "TestAccCloudWatchContributorManagedInsightRulesDataSource_basic",
        ],
    },
    Service {
        name: "ses",
        // SES v1 (Query/XML protocol): the `_basic` smoke for domain identities
        // (+ data source / verification), DKIM, mail-from, identity notification
        // topics, and identity policies. Fixes: identity lookups accept the ARN
        // form (not just the bare name); the domain-verification token is
        // deterministic per identity and stored so verify + get-attributes agree;
        // Get{Verification,MailFrom}Attributes omit unknown identities so the
        // provider's CheckDestroy reads them as gone; DescribeConfigurationSet
        // reports the ReputationOptions block (last_fresh_start / sending_enabled
        // / reputation_metrics_enabled).
        run_regex: "^TestAccSES[A-Za-z0-9]+_basic$",
        deny: &[
            // --- gap: ConfigurationSet and EventDestination pass their apply +
            //     checks but error in the post-apply destroy when a refresh read
            //     hits a configuration set the SDK reports as already gone; the
            //     destroy-time lifecycle for these two still needs work. ---
            "TestAccSESConfigurationSet_basic",
            "TestAccSESEventDestination_basic",
            // --- gap: the template destroy hits the same already-deleted refresh
            //     path (GetTemplate raising TemplateDoesNotExist during destroy). ---
            "TestAccSESTemplate_basic",
            // --- gap: inbound email receipt rules need the SES email-receiving
            //     feature (active rule sets, rule ordering); the receipt tests'
            //     shared PreCheck requires a provisioned rule set that fakecloud
            //     does not model. ---
            "TestAccSESReceiptFilter_basic",
            "TestAccSESReceiptRule_basic",
            "TestAccSESReceiptRuleSet_basic",
        ],
    },
    Service {
        name: "apigateway",
        // API Gateway v1 (REST APIs): the `_basic` smoke across ~40 resources and
        // data sources — REST API, resources, methods, deployments, stages,
        // models, gateway responses, base-path mappings, documentation, usage
        // plans, VPC links, and more. Fixes: GetSdk / GetExport set the
        // Content-Disposition header the SDK/export data sources read as
        // `content_disposition`; UpdateRequestValidator coerces the JSON-Patch
        // string flags back to booleans (validateRequestBody /
        // validateRequestParameters) so GetRequestValidator returns the boolean
        // type the SDK expects.
        run_regex: "^TestAccAPIGateway[A-Za-z0-9]+_basic$",
        deny: &[
            // --- gap: the api-key / usage-plan-key resources expect their
            //     generated `value` / `name` surfaced on read in a way the
            //     resource captures; the create+get responses carry them but the
            //     attribute still reads empty, which needs a closer look. ---
            "TestAccAPIGatewayAPIKey_basic",
            "TestAccAPIGatewayUsagePlanKey_basic",
            // --- gap: update semantics — the method-response model/parameter
            //     update, the integration request_templates replacement, and the
            //     authorizer result-ttl update don't fully apply, leaving drift. ---
            "TestAccAPIGatewayMethodResponse_basic",
            "TestAccAPIGatewayIntegration_basic",
            "TestAccAPIGatewayAuthorizer_basic",
            // --- gap: the REST API resource policy round-trip mangles the policy
            //     JSON (a parse error on apply). ---
            "TestAccAPIGatewayRestAPIPolicy_basic",
            // --- gap: custom domain names need a certificate upload date and the
            //     domain-name access-association resource, which are not modelled. ---
            "TestAccAPIGatewayDomainNameDataSource_basic",
            "TestAccAPIGatewayDomainNameAccessAssociation_basic",
            // --- gap: VPC links provision an NLB; the ELBv2 DeleteLoadBalancer
            //     response omits its result node, so the test's destroy of the
            //     load balancer fails to deserialize (an ELBv2-fidelity gap, not
            //     an API Gateway one). ---
            "TestAccAPIGatewayVPCLink_basic",
            "TestAccAPIGatewayVPCLinkDataSource_basic",
        ],
    },
    Service {
        name: "elbv2",
        // ELBv2 (ALB/NLB) control plane: the `_basic` smoke for target groups
        // (+ data source), listener rules / certificates / data source, trust
        // stores (+ data source / revocation), hosted-zone-id and load-balancer
        // data sources. Fixes: an HTTP/HTTPS target group reports its default
        // ProtocolVersion (HTTP1), and DescribeTargetGroupAttributes returns the
        // cross-zone (`use_load_balancer_configuration`) and anomaly-mitigation
        // (`off`) defaults the data source reads. (DeleteLoadBalancer already
        // returns its result node as of the prior fix.)
        // TargetGroupAttachment now runs: its config launches a real EC2
        // instance from `ConfigLatestAmazonLinux2HVMEBSX8664AMI()`, which the
        // seeded public AMI catalogue (named to match that helper's
        // `amzn2-ami-minimal-hvm-*` + `root-device-type = ebs` filter) resolves.
        run_regex: "^TestAccELBV2[A-Za-z0-9]+_basic$",
        deny: &[],
    },
    Service {
        name: "bedrockagent",
        // Bedrock Agent control plane: the `_basic` smoke for agents, agent
        // action groups, agent aliases, agent collaborators, and the
        // agent-versions data source.
        run_regex: "^TestAccBedrockAgent[A-Za-z0-9]+_basic$",
        deny: &[
            // --- gap: a knowledge-base association requires a real Bedrock
            //     knowledge base, which in turn provisions an Aurora
            //     PostgreSQL (pgvector) cluster as its vector store and runs
            //     embedding ingestion. fakecloud models neither the vector
            //     store nor KB ingestion, so the dependency chain cannot be
            //     stood up. ---
            "TestAccBedrockAgentAgentKnowledgeBaseAssociation_basic",
        ],
    },
    Service {
        name: "appautoscaling",
        // Application Auto Scaling: the `_basic` smoke for scalable targets
        // (`aws_appautoscaling_target`) and scaling policies
        // (`aws_appautoscaling_policy`). Both register an ECS service as the
        // scalable dimension (ecs:service:DesiredCount), so they exercise the
        // ECS control plane on create and the deregister + ECS-service destroy
        // waiters on teardown.
        run_regex: "^TestAccAppAutoScaling[A-Za-z0-9]+_basic$",
        deny: &[],
    },
    Service {
        name: "autoscaling",
        // EC2 Auto Scaling: the `_basic` smoke for an Auto Scaling Group
        // (`aws_autoscaling_group`) and a Launch Configuration
        // (`aws_launch_configuration`). The group launches real container-backed
        // EC2 instances (resolving the launch config's AMI from the seeded
        // catalogue); the launch configuration round-trips InstanceMonitoring /
        // ebs_optimized / spot_price / placement_tenancy, and the group reports
        // ServiceLinkedRoleARN + AvailabilityZoneDistribution the provider reads.
        run_regex: "^TestAccAutoScaling(Group|LaunchConfiguration)_basic$",
        deny: &[],
    },
    Service {
        name: "batch",
        // AWS Batch: the `_basic` smoke for a compute environment
        // (`aws_batch_compute_environment`), a job queue (`aws_batch_job_queue`),
        // and a job definition (`aws_batch_job_definition`). The control plane
        // round-trips the compute-environment status/state, the queue's
        // compute-environment order + priority, and the revisioned job
        // definition the provider reads back on refresh.
        run_regex: "^TestAccBatch(ComputeEnvironment|JobQueue|JobDefinition)_basic$",
        deny: &[],
    },
    Service {
        name: "ssoadmin",
        // IAM Identity Center SSO Admin. fakecloud seeds a default ACTIVE
        // instance at startup so `PreCheckSSOAdminInstances` (which lists
        // instances and skips when none exist) proceeds and the
        // `aws_ssoadmin_instances` data source resolves. The full control plane
        // — permission sets and their inline/managed/customer-managed/boundary
        // policies, applications with access scopes and assignments, trusted
        // token issuers, and the account-assignment async lifecycle — is
        // exercisable. The data sources round-trip against the seeded instance's
        // identity store.
        run_regex: concat!(
            "^TestAccSSOAdmin(",
            "InstancesDataSource",
            "|PermissionSet|PermissionSetInlinePolicy|PermissionSetsDataSource",
            "|PermissionSetDataSource|ManagedPolicyAttachment",
            "|CustomerManagedPolicyAttachment|PermissionsBoundaryAttachment",
            "|Application|ApplicationAccessScope|ApplicationAssignment",
            "|ApplicationAssignmentConfiguration|ApplicationDataSource",
            "|ApplicationProvidersDataSource|ApplicationAssignmentsDataSource",
            "|PrincipalApplicationAssignmentsDataSource|TrustedTokenIssuer",
            "|AccountAssignment",
            ")",
        ),
        deny: &[],
    },
    Service {
        name: "identitystore",
        // IAM Identity Center Identity Store. Resolves the seeded instance's
        // `identity_store_id` via the SSO Admin `aws_ssoadmin_instances` data
        // source, then exercises users, groups, group memberships, and their
        // data sources (unique-attribute + filter lookups). Nested user
        // attribute bags (Name, Emails, Addresses, PhoneNumbers, ...)
        // round-trip.
        run_regex: "^TestAccIdentityStore",
        deny: &[],
    },
    Service {
        name: "verifiedpermissions",
        // AWS Verified Permissions. Exercises policy stores (+ data source),
        // Cedar schemas, static and template-linked policies, policy templates,
        // and Cognito/OIDC identity sources. The `_disappears` tests delete the
        // resource out-of-band and assert it's gone; the identity-source Cognito
        // tests stand up a real `aws_cognito_user_pool` first (fakecloud's
        // Cognito service), then link it. Nested configuration round-trips.
        run_regex: "^TestAccVerifiedPermissions",
        deny: &[],
    },
    Service {
        name: "dms",
        // AWS Database Migration Service control plane. Scoped to the two
        // standalone `_basic` resources that need no VPC / replication-instance
        // co-resources: `aws_dms_certificate` (ImportCertificate + describe +
        // delete, then ImportStateVerify) and `aws_dms_endpoint` (create +
        // in-place update of a source endpoint, with tag add/update/remove).
        // The replication-instance / subnet-group / task / config / S3-endpoint
        // resources and the data sources stand up a VPC + subnets + a settled
        // replication instance first; those come in a later widening batch.
        run_regex: "^TestAccDMS(Certificate|Endpoint)_basic$",
        deny: &[],
    },
    Service {
        name: "cloudtrail",
        // AWS CloudTrail control plane. Scoped to the standalone
        // `aws_cloudtrail_event_data_store` `_basic` resource, whose
        // create/read/update/delete round-trips through the fakecloud control
        // plane (the store settles to ENABLED synchronously, then
        // ImportStateVerify re-reads it). The plain `aws_cloudtrail` trail
        // resource tests are grouped under the `TestAccCloudTrail_serial`
        // super-test (Trail/*) and, together with channels, imports, Lake
        // queries, dashboards and organization delegated admin, come in a
        // later widening batch.
        run_regex: "^TestAccCloudTrailEventDataStore_basic$",
        deny: &[],
    },
    Service {
        name: "transfer",
        // AWS Transfer Family control plane. Scoped to the standalone `_basic`
        // resources whose create/read/update/delete round-trips through the
        // fakecloud control plane: `aws_transfer_server` (SERVICE_MANAGED
        // server that settles to ONLINE), `aws_transfer_user` +
        // `aws_transfer_ssh_key` (a user and an imported SSH public key on that
        // server), and `aws_transfer_access` (a directory-group access). Wider
        // resources (connectors, agreements, profiles, certificates, workflows,
        // web apps) come in a later widening batch.
        run_regex: "^TestAccTransfer(Server|User|SSHKey|Access)_basic$",
        deny: &[],
    },
    Service {
        name: "appconfig",
        // AWS AppConfig control plane. Scoped to the standalone `_basic`
        // resources whose create/read/update/delete round-trips through the
        // fakecloud control plane: `aws_appconfig_application`,
        // `aws_appconfig_environment`, `aws_appconfig_configuration_profile`,
        // `aws_appconfig_deployment_strategy`, and `aws_appconfig_extension`.
        run_regex:
            "^TestAccAppConfig(Application|Environment|ConfigurationProfile|DeploymentStrategy|Extension)_basic$",
        deny: &[],
    },
    Service {
        name: "codeconnections",
        // AWS CodeConnections control plane. The `_basic` resources whose
        // create/read/update/delete round-trips through the fakecloud control
        // plane: `aws_codeconnections_connection` (created PENDING, matching the
        // real handshake-pending default) and `aws_codeconnections_host`
        // (settles to PENDING, a valid target state for the provider's create
        // waiter).
        run_regex: "^TestAccCodeConnections(Connection|Host)_basic$",
        deny: &[],
    },
    Service {
        name: "codebuild",
        // AWS CodeBuild control plane. Scoped to the standalone `_basic`
        // resources whose create/read/update/delete round-trips through the
        // fakecloud control plane: `aws_codebuild_project`,
        // `aws_codebuild_report_group`, `aws_codebuild_source_credential`,
        // `aws_codebuild_fleet`, and `aws_codebuild_resource_policy`. Webhooks
        // and data sources come in a later widening batch.
        run_regex:
            "^TestAccCodeBuild(Project|ReportGroup|SourceCredential|Fleet|ResourcePolicy)_basic$",
        deny: &[],
    },
    Service {
        // AWS CodeCommit control plane. Scoped to the `_basic` resources whose
        // create/read/update/delete round-trips through the fakecloud control
        // plane: `aws_codecommit_repository`, `aws_codecommit_trigger` (its
        // destination is an SNS topic provisioned against fakecloud's own SNS),
        // `aws_codecommit_approval_rule_template`, and
        // `aws_codecommit_approval_rule_template_association`.
        name: "codecommit",
        run_regex:
            "^TestAccCodeCommit(Repository_basic|Trigger_basic|ApprovalRuleTemplate_basic|ApprovalRuleTemplateAssociation_basic)$",
        deny: &[],
    },
    Service {
        // AWS CodeDeploy control plane. The upstream Go package is `deploy`
        // (its resources are `aws_codedeploy_*`). Scoped to the `_basic`
        // resources whose create/read/update/delete round-trips through the
        // fakecloud control plane: `aws_codedeploy_app`,
        // `aws_codedeploy_deployment_config`, and
        // `aws_codedeploy_deployment_group`.
        name: "deploy",
        run_regex: "^TestAccDeploy(App|DeploymentConfig|DeploymentGroup)_basic$",
        deny: &[],
    },
    Service {
        // AWS CodePipeline control plane. Scoped to the `_basic` resources
        // whose create/read/update/delete round-trips through the fakecloud
        // control plane: `aws_codepipeline`, `aws_codepipeline_webhook`, and
        // `aws_codepipeline_custom_action_type`. A pipeline requires an S3
        // artifactStore and an IAM service role, both of which the upstream
        // test provisions against fakecloud's own S3 + IAM.
        name: "codepipeline",
        run_regex: "^TestAccCodePipeline(_basic|Webhook_basic|CustomActionType_basic)$",
        deny: &[],
    },
    Service {
        // AWS CodeArtifact control plane. The upstream package drives every
        // resource through one serial umbrella test
        // (`TestAccCodeArtifact_serial`) whose two-level subtest map groups the
        // `basic` cases per resource; the `-run` path scopes to the `basic`
        // subtest of the four control-plane resources: `aws_codeartifact_domain`,
        // `aws_codeartifact_repository`, `aws_codeartifact_domain_permissions_policy`,
        // and `aws_codeartifact_repository_permissions_policy`. A domain requires
        // a KMS key, which the upstream test provisions against fakecloud's KMS.
        name: "codeartifact",
        run_regex:
            "^TestAccCodeArtifact_serial$/^(Domain|Repository|DomainPermissionsPolicy|RepositoryPermissionsPolicy)$/^basic$",
        deny: &[],
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
        name: "organizations",
        service: "organizations",
        run_regex: "^TestAccOrganizations[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "ecr",
        service: "ecr",
        run_regex: "^TestAccECR[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "glue",
        service: "glue",
        run_regex: "^TestAccGlue[A-Za-z]+_basic$",
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
        run_regex:
            "^TestAccAPIGatewayV2([A-Za-z]+_basic|API_basicHTTP|Integration_basic(HTTP|WebSocket))$",
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
    // (+ data source), the table data source, contributor insights, tagging,
    // kinesis streaming destination (optional precision now echoes empty), and
    // resource policy (revision id is derived from the policy content, so
    // import-state-verify round-trips). The remaining non-table resources are
    // deliberately omitted, not denied:
    //   * global_table / table_replica — cross-region replication,
    //   * table_export — S3 export path,
    // each of which needs a dedicated fix and will be added later.
    Shard {
        name: "dynamodb-resources",
        service: "dynamodb",
        run_regex: concat!(
            "^TestAccDynamoDB(",
            "ContributorInsights|Tag|TableItem|TableItemDataSource|TableDataSource",
            "|KinesisStreamingDestination|ResourcePolicy",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    // ─── Wave 3 tail (one shard each, narrow positive regex) ───────────
    Shard {
        name: "cloudformation",
        service: "cloudformation",
        run_regex: "^TestAccCloudFormation(ExportDataSource|Stack)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "cognitoidentity",
        service: "cognitoidentity",
        run_regex: concat!(
            "^TestAccCognitoIdentity(",
            "Pool|PoolDataSource|PoolRolesAttachment",
            "|OpenIDTokenForDeveloperIdentityEphemeral",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "lambda",
        service: "lambda",
        run_regex: concat!(
            "^TestAccLambda(",
            "FunctionURL|FunctionURLDataSource",
            "|LayerVersion|LayerVersionDataSource|ProvisionedConcurrencyConfig",
            "|Alias|FunctionDataSource|FunctionEventInvokeConfig",
            "|Function|Permission|FunctionsDataSource|RuntimeManagementConfig",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "ecs",
        service: "ecs",
        run_regex: concat!(
            "^TestAccECS(",
            "ClusterCapacityProviders|ClustersDataSource",
            "|TaskDefinition|TaskExecutionDataSource",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "ec2",
        service: "ec2",
        run_regex: concat!(
            "^TestAcc(",
            "EC2(EIP|EIPsDataSource|KeyPair)",
            "|VPC(|Subnet|SecurityGroup|RouteTable|InternetGateway|NetworkACL",
            "|NetworkInterface|DHCPOptions|EgressOnlyInternetGateway",
            "|DefaultSecurityGroup|DefaultNetworkACL|Route",
            "|MainRouteTableAssociation|NetworkACLAssociation)",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    // NAT gateways, VPC peering connections (+ their modifiable peering
    // options), DHCP-options-set associations, and flow logs. Split out of
    // the core `ec2` shard to keep that job's wall time down and isolate
    // these newer families. All run purely in the control plane (no Docker).
    Shard {
        name: "ec2-vpc2",
        service: "ec2",
        run_regex: concat!(
            "^TestAcc(",
            "VPCNATGateway",
            "|VPCPeeringConnection|VPCPeeringConnectionOptions",
            "|VPCDHCPOptionsAssociation",
            "|VPCFlowLog",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "rds",
        service: "rds",
        run_regex: "^TestAccRDSSubnetGroup_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "redshift",
        service: "redshift",
        run_regex: "^TestAccRedshift[A-Za-z]+_basic$",
        extra_deny: &[],
    },
    // DB and DB-cluster parameter groups. Both need no engine container:
    // the create/modify/reset/describe round-trip now preserves each
    // parameter's ApplyMethod, resets cleanly back to engine-default, and
    // (for cluster groups) emits the named member tag + Description that
    // the provider reads back.
    Shard {
        name: "rds-param-groups",
        service: "rds",
        run_regex: "^TestAccRDS(ParameterGroup|ClusterParameterGroup)_basic$",
        extra_deny: &[],
    },
    // DB option groups. DescribeOptionGroups now emits the named
    // <OptionGroup> member tag (not <member>) so the SDK reads it back, plus
    // the OptionGroupDescription/VPC-membership fields the provider asserts.
    Shard {
        name: "rds-option-groups",
        service: "rds",
        run_regex: "^TestAccRDSOptionGroup_basic$",
        extra_deny: &[],
    },
    // Event subscriptions and global clusters. Both Describe lists now use
    // their named member tags (<EventSubscription> / <GlobalClusterMember>)
    // and the Create responses wrap the resource element, so the provider
    // no longer nil-derefs reading them back.
    Shard {
        name: "rds-event-global",
        service: "rds",
        run_regex: "^TestAccRDS(EventSubscription|GlobalCluster)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "elasticache",
        service: "elasticache",
        run_regex: concat!(
            "^TestAccElastiCache(",
            "User|UserDataSource",
            "|UserGroup|UserGroupAssociation",
            "|SubnetGroup|SubnetGroupDataSource",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "elasticbeanstalk",
        service: "elasticbeanstalk",
        run_regex: concat!(
            "^TestAccElasticBeanstalk(",
            "Application_basic",
            "|ConfigurationTemplate_basic",
            "|Environment_basic",
            ")$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "memorydb",
        service: "memorydb",
        run_regex: concat!(
            "^TestAccMemoryDB(",
            "ACL|User|ParameterGroup|Snapshot|Cluster",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "servicediscovery",
        service: "servicediscovery",
        run_regex: "^TestAccServiceDiscovery",
        extra_deny: &[],
    },
    Shard {
        name: "account",
        service: "account",
        run_regex: "^TestAccAccount_serial$",
        extra_deny: &[],
    },
    Shard {
        name: "eks",
        service: "eks",
        run_regex: concat!(
            "^TestAccEKS(",
            "Cluster|ClusterDataSource|ClustersDataSource|ClusterVersionsDataSource",
            "|AccessEntry|AccessEntryDataSource|AccessPolicyAssociation",
            "|IdentityProviderConfig|PodIdentityAssociation",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "efs",
        service: "efs",
        run_regex: concat!(
            "^TestAccEFS(",
            "FileSystem|MountTarget|AccessPoint|BackupPolicy|FileSystemPolicy",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "mq",
        service: "mq",
        run_regex: "^TestAccMQ(Broker|Configuration)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "glacier",
        service: "glacier",
        run_regex:
            "^TestAccGlacier(Vault_basic|Vault_notification|Vault_policy|Vault_tags|VaultLock_basic)$",
        extra_deny: &[],
    },
    Shard {
        name: "backup",
        service: "backup",
        run_regex: "^TestAccBackup(Vault|Plan|Selection|Framework|ReportPlan)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "elasticsearch",
        service: "elasticsearch",
        run_regex: "^TestAccElasticsearchDomain_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "opensearch",
        service: "opensearch",
        run_regex: "^TestAccOpenSearchDomain_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "cloudfront",
        service: "cloudfront",
        run_regex: concat!(
            "^TestAccCloudFront(",
            "CachePolicyDataSource|OriginRequestPolicyDataSource",
            "|RealtimeLogConfigDataSource|LogDeliveryCanonicalUserIDDataSource",
            ")_basic$",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "sfn",
        service: "sfn",
        run_regex: "^TestAccSFN(StateMachineDataSource|Activity|ActivityDataSource)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "scheduler",
        service: "scheduler",
        run_regex: "^TestAccScheduler[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    // EventBridge Pipes is split across three shards, each a fresh fakecloud
    // handling at most four pipe-lifecycle tests. Pipes is the only service that
    // runs a persistent execution loop, and a single process that drives ~6+ of
    // these create/transition/destroy tests back-to-back on a constrained 2-core
    // CI runner degrades until the provider's delete/"gone" waiters stop
    // converging (a pipe or its SQS source/target queue lingers past the waiter
    // budget). Keeping each shard small holds every run well under that point;
    // the union of the three equals the original single-shard selection.
    Shard {
        name: "pipes-a",
        service: "pipes",
        run_regex: "^TestAccPipesPipe_(basicSQS|disappears|description|desiredState)$",
        extra_deny: &[],
    },
    Shard {
        name: "pipes-b",
        service: "pipes",
        run_regex: "^TestAccPipesPipe_(roleARN|nameGenerated|namePrefix|tags)$",
        extra_deny: &[],
    },
    Shard {
        name: "pipes-c",
        service: "pipes",
        run_regex: "^TestAccPipesPipe_(targetUpdate|sourceParameters_filterCriteria|targetParameters_inputTemplate)$",
        extra_deny: &[],
    },
    Shard {
        name: "sesv2",
        service: "sesv2",
        run_regex: "^TestAccSESV2[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "wafv2",
        service: "wafv2",
        run_regex: "^TestAccWAFV2[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "firehose",
        service: "firehose",
        run_regex: "^TestAccFirehoseDeliveryStream(_basic|DataSource_basic)$",
        extra_deny: &[],
    },
    Shard {
        name: "cloudwatch",
        service: "cloudwatch",
        run_regex: "^TestAccCloudWatch[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "ses",
        service: "ses",
        run_regex: "^TestAccSES[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "apigateway",
        service: "apigateway",
        run_regex: "^TestAccAPIGateway[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "elbv2",
        service: "elbv2",
        run_regex: "^TestAccELBV2[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "bedrockagent",
        service: "bedrockagent",
        run_regex: "^TestAccBedrockAgent[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "appautoscaling",
        service: "appautoscaling",
        run_regex: "^TestAccAppAutoScaling[A-Za-z0-9]+_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "autoscaling",
        service: "autoscaling",
        run_regex: "^TestAccAutoScaling(Group|LaunchConfiguration)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "batch",
        service: "batch",
        run_regex: "^TestAccBatch(ComputeEnvironment|JobQueue|JobDefinition)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "ssoadmin",
        service: "ssoadmin",
        run_regex: concat!(
            "^TestAccSSOAdmin(",
            "InstancesDataSource",
            "|PermissionSet|PermissionSetInlinePolicy|PermissionSetsDataSource",
            "|PermissionSetDataSource|ManagedPolicyAttachment",
            "|CustomerManagedPolicyAttachment|PermissionsBoundaryAttachment",
            "|Application|ApplicationAccessScope|ApplicationAssignment",
            "|ApplicationAssignmentConfiguration|ApplicationDataSource",
            "|ApplicationProvidersDataSource|ApplicationAssignmentsDataSource",
            "|PrincipalApplicationAssignmentsDataSource|TrustedTokenIssuer",
            "|AccountAssignment",
            ")",
        ),
        extra_deny: &[],
    },
    Shard {
        name: "identitystore",
        service: "identitystore",
        run_regex: "^TestAccIdentityStore",
        extra_deny: &[],
    },
    Shard {
        name: "verifiedpermissions",
        service: "verifiedpermissions",
        run_regex: "^TestAccVerifiedPermissions",
        extra_deny: &[],
    },
    Shard {
        name: "dms",
        service: "dms",
        run_regex: "^TestAccDMS(Certificate|Endpoint)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "transfer",
        service: "transfer",
        run_regex: "^TestAccTransfer(Server|User|SSHKey|Access)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "appconfig",
        service: "appconfig",
        run_regex:
            "^TestAccAppConfig(Application|Environment|ConfigurationProfile|DeploymentStrategy|Extension)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "cloudtrail",
        service: "cloudtrail",
        run_regex: "^TestAccCloudTrailEventDataStore_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "ram",
        service: "ram",
        run_regex: "^TestAccRAM(ResourceShare_basic|ResourceShare_tags|PrincipalAssociation_basic|ResourceAssociation_basic)$",
        extra_deny: &[],
    },
    Shard {
        name: "ce",
        service: "ce",
        run_regex: "^TestAccCE(AnomalyMonitor_basic|AnomalySubscription_basic|CostCategory_basic|CostAllocationTag_basic)$",
        extra_deny: &[],
    },
    Shard {
        name: "s3tables",
        service: "s3tables",
        run_regex: "^TestAccS3Tables(TableBucket|Namespace|Table|TableBucketPolicy|TablePolicy)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "lakeformation",
        service: "lakeformation",
        run_regex: "^TestAccLakeFormation(Resource_basic|Resource_hybridAccessEnabled|_serial)$/^(DataLakeSettings|LFTags)$/^basic$",
        extra_deny: &[],
    },
    Shard {
        name: "codeconnections",
        service: "codeconnections",
        run_regex: "^TestAccCodeConnections(Connection|Host)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "codebuild",
        service: "codebuild",
        run_regex:
            "^TestAccCodeBuild(Project|ReportGroup|SourceCredential|Fleet|ResourcePolicy)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "codecommit",
        service: "codecommit",
        run_regex:
            "^TestAccCodeCommit(Repository_basic|Trigger_basic|ApprovalRuleTemplate_basic|ApprovalRuleTemplateAssociation_basic)$",
        extra_deny: &[],
    },
    Shard {
        name: "codedeploy",
        service: "deploy",
        run_regex: "^TestAccDeploy(App|DeploymentConfig|DeploymentGroup)_basic$",
        extra_deny: &[],
    },
    Shard {
        name: "codepipeline",
        service: "codepipeline",
        run_regex: "^TestAccCodePipeline(_basic|Webhook_basic|CustomActionType_basic)$",
        extra_deny: &[],
    },
    Shard {
        name: "codeartifact",
        service: "codeartifact",
        run_regex:
            "^TestAccCodeArtifact_serial$/^(Domain|Repository|DomainPermissionsPolicy|RepositoryPermissionsPolicy)$/^basic$",
        extra_deny: &[],
    },
];
