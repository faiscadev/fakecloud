+++
title = "AWS IoT Wireless"
description = "AWS IoT Wireless (iotwireless) on fakecloud: the full 112-operation LoRaWAN + Sidewalk control plane — destinations, device/service profiles, FUOTA tasks, multicast groups, wireless devices + gateways — at 100% conformance. restJson1."
weight = 79
+++

fakecloud implements the **AWS IoT Wireless control plane** (`iotwireless`, SDK
id `IoT Wireless`, endpoint prefix `api.iotwireless`) as a restJson1 service.
All **112 operations** ship with **100% conformance** against AWS's own Smithy
model, backed by account-partitioned state that persists across restarts in
persistent mode. IoT Wireless signs SigV4 with the `iotwireless` scope and is
routed by HTTP method plus `@http` URI path (`POST /destinations`,
`GET /wireless-devices/{Identifier}`, `PATCH /fuota-tasks/{Id}`,
`DELETE /multicast-groups/{Id}`, ...).

The route table, the per-operation HTTP bindings, the model-derived input
constraints, and the output member shapes are all generated directly from the
Smithy model (`scripts/generate-iotwireless-tables.py`), so the registry tracks
the model exactly rather than by hand.

## Collection-POST creates

Unlike AWS IoT Core (where a create is `POST /things/{thingName}` and the name
is a URI label), IoT Wireless models every create as a **collection `POST`**
(`POST /destinations`, `POST /device-profiles`, ...) and the new resource's
identity comes from the response:

- Families whose create output carries an `Id` mint a UUID-shaped **`Id`**:
  device profiles, service profiles, FUOTA tasks, multicast groups, wireless
  devices, wireless gateways, and wireless-gateway task definitions.
- The two name-addressed families — **destinations** and **network-analyzer
  configurations** — key off the required body **`Name`**.

The matching read / update / delete carry the identifier as a path label, so
every verb of a family resolves to one persisted record.

## What is real

Every modelled named resource mints proper ARNs and ids, persists its
attributes, and round-trips on read / list / update with paginating
`NextToken`s:

- **Destinations** — create / get / list / update / delete, round-tripping the
  `Expression` / `ExpressionType` / `RoleArn` / `Description`.
- **Device profiles**, **service profiles**, **FUOTA tasks**, **multicast
  groups**, **wireless devices**, **wireless gateways**, and **wireless-gateway
  task definitions** — server-assigned ids, ARNs, and numeric `CreatedAt`
  timestamps.
- **Network-analyzer configurations** — name-addressed create + get / update /
  list / delete.
- **Position configurations** — the `PutPositionConfiguration` /
  `GetPositionConfiguration` pair, keyed by resource identifier.
- **Resource positions** — `UpdateResourcePosition` / `GetResourcePosition`
  store and return the raw GeoJSON `@httpPayload` blob.
- **Tags** — ARN-keyed `TagResource` / `ListTagsForResource` / `UntagResource`.

Input validation is model-derived: `required` members (including a required
`@httpPayload`), string `@length`, numeric `@range`, and `@enum` constraints are
enforced with IoT Wireless's declared exceptions (`ValidationException`,
`ResourceNotFoundException`, `ConflictException`, ...).

### Timestamps

restJson1 wire-encodes timestamps as **epoch-seconds JSON numbers** (e.g.
`1752324947.041`), not RFC3339 strings — the aws-sdk timestamp deserializer
rejects a string. Every server-generated timestamp (`CreatedAt`,
`CreationTime`, ...) is emitted as a number and round-trips through create ->
read as the same numeric value.

## Honest emulation choices (documented, not stubbed)

- There is **no live LoRaWAN / Sidewalk radio plane**. The control plane is
  fully real and persisted, but no device ever transmits, no downlink is
  delivered (`SendDataToWirelessDevice` / `SendDataToMulticastGroup`), no FUOTA
  firmware image is fragmented, and no gateway task runs. Association and
  session operations are accepted (and persisted where addressed by a stored
  resource) but drive no radio behaviour.
