+++
title = "AWS IoT Data Plane"
description = "AWS IoT Data Plane (iotdata) on fakecloud: device shadows + retained messages, a complete 11-operation data plane (100% conformance). restJson1."
weight = 79
+++

fakecloud implements the **AWS IoT Data Plane** (`iotdata`, SDK id
`IoT Data Plane`, endpoint prefix `data-ats.iot`) as a restJson1 service. All
**11 operations** ship with **100% conformance** against AWS's own Smithy model,
backed by account-partitioned state that persists across restarts in persistent
mode. IoT Data Plane signs SigV4 with the `iotdata` scope and is routed by HTTP
method plus `@http` URI path (`POST /things/{thingName}/shadow`,
`GET /things/{thingName}/shadow`, `POST /topics/{topic}`,
`GET /retainedMessage/{topic}`, ...).

This is a real device-shadow state machine and retained-message store, not a set
of stubs: `UpdateThingShadow` really merges, versions, and computes deltas;
`GetThingShadow` returns exactly what was merged; and retained messages are
stored and read back byte-for-byte.

## Device shadows

A **shadow** is a persistent JSON document representing a thing's state. Each
thing has one classic (unnamed) shadow plus any number of **named** shadows.
Shadows are stored by `thingName` string; fakecloud does not require the thing
to pre-exist in an IoT Core registry.

- **`UpdateThingShadow`** (`POST /things/{thingName}/shadow`, optional
  `?name=<shadowName>`) accepts a shadow document as the raw `@httpPayload`
  body: `{"state":{"desired":{…},"reported":{…}}}`. The update is **deep-merged**
  into the stored shadow (a `null` leaf **deletes** that key, matching AWS;
  nested objects merge recursively, everything else overwrites). On each
  successful update the `version` increments by one, per-leaf `metadata`
  timestamps are stamped for every touched leaf, and `state.delta` (the subset
  of `desired` that differs from `reported`) is recomputed. The response payload
  is the resulting document.
- **`GetThingShadow`** (`GET /things/{thingName}/shadow`, optional `?name=`)
  returns the full shadow document: `state` (`desired` / `reported` / `delta`),
  `metadata`, `version`, and `timestamp`. A missing shadow is
  `ResourceNotFoundException`.
- **`DeleteThingShadow`** (`DELETE /things/{thingName}/shadow`, optional `?name=`)
  removes the shadow and returns the deletion payload (`version` + `timestamp`).
- **`ListNamedShadowsForThing`**
  (`GET /api/things/shadow/ListNamedShadowsForThing/{thingName}`) returns a
  thing's named-shadow names, paginated by `pageSize` with a round-tripping
  `nextToken`.

### Version conflicts

An `UpdateThingShadow` may carry a `version` for optimistic locking. If it does
not match the shadow's stored version, the update is rejected with
`ConflictException` (HTTP 409).

## Publish + retained messages

- **`Publish`** (`POST /topics/{topic}`, `?qos=`, `?retain=`) accepts an MQTT
  message for a topic. When `retain=true` the message is stored (an empty
  retained payload **clears** the topic, matching AWS); a non-retained publish
  is accepted as a 200 no-op.
- **`GetRetainedMessage`** (`GET /retainedMessage/{topic}`) returns a stored
  retained message (`payload`, `qos`, `lastModifiedTime`), or
  `ResourceNotFoundException` when the topic has none.
- **`ListRetainedMessages`** (`GET /retainedMessage`) paginates the
  retained-topic summaries (`topic`, `payloadSize`, `qos`, `lastModifiedTime`)
  by `maxResults`.

## Validation & errors

Model-derived validation rejects contract violations with the codes the model
declares: `InvalidRequestException` (malformed shadow document, out-of-range
`qos`), `ResourceNotFoundException` (missing shadow / retained message /
connection), `ConflictException` (shadow version conflict), plus the declared
`ThrottlingException` / `UnauthorizedException` / `MethodNotAllowedException` /
`RequestEntityTooLargeException` / `ServiceUnavailableException` /
`InternalFailureException` family. State is account-partitioned and persisted.

## Honest gaps

fakecloud runs **no MQTT broker**, which shapes two areas honestly:

- **No live pub/sub fan-out.** `Publish` is accepted (and, for retained
  messages, stored), but it is never delivered to subscribed MQTT clients. There
  is no live broker to fan messages out to.
- **No connected clients.** Because there is no broker, no MQTT client is ever
  *connected*. The connection-introspection operations (`GetConnection`,
  `DeleteConnection`, `ListSubscriptions`, and `SendDirectMessage`) therefore
  faithfully return `ResourceNotFoundException` for every client id, which is
  exactly what AWS returns for an unknown connection. This is a real response
  reflecting the emulated state, not a fabricated connection record.

The device-shadow merge/delta/version state machine and the retained-message
store are fully real; these gaps are confined to live MQTT transport, which has
no in-process equivalent.
