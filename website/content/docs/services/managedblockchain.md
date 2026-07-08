+++
title = "Amazon Managed Blockchain"
description = "Amazon Managed Blockchain (managedblockchain) on fakecloud: a complete 27-operation control plane for networks, members, nodes, proposals, voting, invitations, and accessors (100% conformance). restJson1."
weight = 78
+++

fakecloud implements **Amazon Managed Blockchain** as a restJson1 service. All
**27 operations** ship with **100% conformance** against AWS's own Smithy model,
backed by account-partitioned state that persists across restarts in persistent
mode. Managed Blockchain signs SigV4 with the `managedblockchain` scope and is
routed by HTTP method plus `@http` URI path (`POST /networks`,
`GET /networks/{NetworkId}/members/{MemberId}`,
`POST /networks/{NetworkId}/proposals/{ProposalId}/votes`,
`POST /accessors`, ...).

Amazon Managed Blockchain on fakecloud is a faithful **control plane**: every
`CreateNetwork` / `CreateMember` / `CreateNode` / `CreateProposal` /
`CreateAccessor` is reflected by its `Get*` / `List*`, the proposal/voting state
machine actually runs, and approved invitation proposals materialise real
`Invitation` records.

## No real blockchain runs (documented honest gap, not stubbed)

Managed Blockchain, by design, is a managed control plane in front of
Hyperledger Fabric and Ethereum networks. fakecloud does **not** run a real
Fabric orderer, certificate authority, peer, or an Ethereum node. The
endpoints it returns -- the network `OrderingServiceEndpoint`, each member's
Fabric `CaEndpoint`, each node's Fabric `PeerEndpoint` / `PeerEventEndpoint`,
and each Ethereum node's `HttpEndpoint` / `WebSocketEndpoint` -- are well-formed,
deterministic URLs derived from the resource ids, but **nothing is listening
behind them**: there is no orderer, CA, peer, or JSON-RPC endpoint serving a live
chain. Everything that Managed Blockchain's *API* owns is real, account-
partitioned, and persisted; only the blockchain data plane those endpoints would
front is absent. This is the honest treatment: clients that create networks,
manage members and nodes, drive proposals to approval, and mint token accessors
behave exactly as against AWS, but no ledger is committed.

## Resources

- **Networks** - `CreateNetwork` (`POST /networks`) validates the required
  `Name`, `Framework` (`HYPERLEDGER_FABRIC` / `ETHEREUM`), `FrameworkVersion`,
  `VotingPolicy`, and `MemberConfiguration`, mints an `n-<26 uppercase alnum>`
  id and a global `arn:aws:managedblockchain:::networks/<id>` ARN, and stores the
  framework configuration + voting policy. For Hyperledger Fabric it atomically
  creates the requested first member (`m-...`), returning both `NetworkId` and
  `MemberId`; the network is `AVAILABLE`. `GetNetwork`, `ListNetworks` (filtered
  by `name` / `framework` / `status`, paginated with a round-tripping
  `nextToken`).
- **Members** - full CRUD of `m-...` members, each carrying Fabric framework
  attributes (`CaEndpoint`, `AdminUsername`) and an optional log-publishing
  configuration. A member is created `CREATING` and settles to `AVAILABLE` on the
  next read; `DeleteMember` flips it to `DELETED`. `UpdateMember` /
  `GetMember` / `ListMembers`.
- **Nodes** - full CRUD of `nd-...` nodes carrying `InstanceType`,
  `AvailabilityZone`, `StateDB` (`LevelDB` / `CouchDB`), and deterministically
  derived framework endpoints (Fabric peer endpoints, or Ethereum HTTP +
  WebSocket endpoints). `CREATING` settles to `AVAILABLE`; `UpdateNode`,
  `DeleteNode`, `GetNode`, `ListNodes`.
- **Proposals + voting** - `CreateProposal` mints `p-...`, stores the `Actions`
  (`Invitations` / `Removals`), sets `IN_PROGRESS`, and derives an
  `ExpirationDate` from the network's `ProposalDurationInHours`. `VoteOnProposal`
  records a `YES` / `NO` vote per member; when the `ApprovalThresholdPolicy`
  threshold is met the proposal transitions to `APPROVED` -- materialising each
  invitation into a real `Invitation` in the invited principal's account and
  applying removals -- or `REJECTED` once approval is arithmetically impossible.
  `ListProposalVotes`, `GetProposal`, `ListProposals`.
- **Invitations** - `ListInvitations` returns pending invitations for the
  calling account; `RejectInvitation` flips one to `REJECTED`.
- **Accessors** - `CreateAccessor` mints a UUID accessor id, a `BillingToken`,
  and an ARN (`AccessorType` `BILLING_TOKEN`, status `AVAILABLE`). `GetAccessor`,
  `ListAccessors` (filtered by `networkType`), `DeleteAccessor`
  (`PENDING_DELETION`).
- **Tagging** - ARN-keyed `TagResource` (`POST /tags/{ResourceArn}`),
  `UntagResource`, `ListTagsForResource`.

## Validation

Model-derived input validation rejects contract violations (missing required
members, out-of-set enums) with the codes each operation declares --
`InvalidRequestException`, `ResourceNotFoundException`, `IllegalActionException`
-- so a malformed request lands on a declared error rather than a routing miss.
