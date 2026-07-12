+++
title = "AWS IoT Core"
description = "AWS IoT Core (iot) on fakecloud: the full 272-operation control plane — things, policies, certificates, jobs, topic rules, Device Defender, provisioning — at 100% conformance. restJson1."
weight = 78
+++

fakecloud implements the **AWS IoT Core control plane** (`iot`, SDK id `IoT`,
endpoint prefix `iot`) as a restJson1 service. All **272 operations** ship with
**100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode. IoT
Core signs SigV4 with the `iot` scope and is routed by HTTP method plus `@http`
URI path (`POST /things/{thingName}`, `GET /policies/{policyName}`,
`PUT /jobs/{jobId}`, `POST /rules/{ruleName}`, ...).

The route table, the per-operation HTTP bindings, the model-derived input
constraints, and the output member shapes are all generated directly from the
Smithy model, so the registry / jobs / rules / security control plane tracks the
model exactly rather than by hand.

## What is real

Every modelled resource family mints proper ARNs and ids, persists its
attributes, echoes them back on read / list with round-tripping pagination
tokens, and enforces referential rules:

- **Things**, **thing types**, **thing groups** (static + dynamic), and
  **billing groups** — create / describe / update / delete / list, plus
  thing-group and billing-group membership (`AddThingToThingGroup`,
  `AddThingToBillingGroup`, and their `Remove`/`List` counterparts).
- **Policies** — create / get / delete / list, versions, default-version
  selection, and both v2 (`AttachPolicy` / `ListAttachedPolicies` /
  `ListTargetsForPolicy`) and legacy principal (`AttachPrincipalPolicy` /
  `ListPrincipalPolicies`) attachments.
- **Certificates** — `CreateKeysAndCertificate` and `CreateCertificateFromCsr`
  mint a 64-hex certificate id, an ARN, a structurally-shaped PEM certificate,
  and (for keys-and-certificate) an RSA key pair; `RegisterCertificate`,
  `RegisterCACertificate`, describe / update / delete / list, transfer, and
  thing-principal attachment (`AttachThingPrincipal` / `ListThingPrincipals`)
  all persist and round-trip.
- **Jobs** and **job templates**, **topic rules** and **rule destinations**
  (the rule payload's SQL + actions are stored verbatim; `Enable`/`Disable`
  flip the rule's disabled flag), **Device Defender** (security profiles,
  scheduled audits, account audit configuration, mitigation actions, custom
  metrics, dimensions), **provisioning templates** (+ versions), **domain
  configurations**, **fleet metrics**, **role aliases**, **authorizers**
  (+ default authorizer), **streams**, **OTA updates**, **packages**
  (+ versions), **certificate providers**, and **commands**.
- **Endpoints** — `DescribeEndpoint` returns a deterministic account-specific
  host for every endpoint type (`iot:Data-ATS`, `iot:CredentialProvider`,
  `iot:Jobs`, ...).
- **Tagging** — `TagResource` / `UntagResource` / `ListTagsForResource` keyed
  by ARN.

Input validation is model-derived: required members, string `@length`, numeric
`@range`, and `@enum` constraints are enforced with each operation's declared
exceptions (`ResourceNotFoundException`, `InvalidRequestException`,
`ResourceAlreadyExistsException`, `VersionConflictException`,
`DeleteConflictException`, ...). State is account-partitioned and persisted.

## Honest gaps

These are documented emulation choices, not stubs:

- **No live MQTT broker or device connectivity.** The registry / jobs / rules /
  security *control plane* is fully real and persisted, but no message is
  routed, no topic-rule action is executed against a real target (SNS / SQS /
  Lambda / ...), and no device ever attaches over MQTT. The device *data plane*
  (shadows + retained messages) lives in the separate
  [`iotdata`](@/docs/services/iotdata.md) service.
- **Bounded fleet-index search.** `SearchIndex` and the aggregation queries
  (`GetCardinality`, `GetPercentiles`, `GetStatistics`, `GetBucketsAggregation`)
  run against the in-memory thing registry with a bounded query subset (the
  wildcard `*`, `thingName:<name>`, or a bare thing name). A query outside that
  subset returns `InvalidQueryException` rather than a wrong result.
- **Certificates are structurally-valid PEM placeholders**, not real CA-signed
  X.509 chains; the key pair is a shaped placeholder, not a usable private key.
