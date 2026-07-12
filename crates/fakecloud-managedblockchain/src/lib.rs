//! Amazon Managed Blockchain (`managedblockchain`) restJson1 control plane for
//! fakecloud.
//!
//! The full 27-operation Amazon Managed Blockchain Smithy model. Managed
//! Blockchain signs SigV4 with the `managedblockchain` scope and speaks
//! restJson1; every operation is a RESTful `<METHOD> /...` route with path
//! labels (e.g. `POST /networks`, `GET /networks/{NetworkId}/members/{MemberId}`),
//! so requests are routed by their HTTP method + `@http` URI template.
//!
//! This is real, persisted, account-partitioned control-plane state, not a set
//! of stubs:
//!
//! * **Networks.** `CreateNetwork` mints a `n-...` id + ARN and stores the
//!   name, description, framework (`HYPERLEDGER_FABRIC` / `ETHEREUM`), framework
//!   version, framework configuration and voting policy. For Hyperledger Fabric
//!   it atomically creates the requested first member (`m-...`), returning both
//!   `NetworkId` and `MemberId`; the network is `AVAILABLE`. `GetNetwork` /
//!   `ListNetworks` (filtered by name/framework/status, paginated) round-trip.
//! * **Members.** Full CRUD of `m-...` members, each with Fabric framework
//!   attributes (CA endpoint + admin username) and a log-publishing config;
//!   `CREATING` settles to `AVAILABLE` on read, `DeleteMember` flips to
//!   `DELETED`.
//! * **Nodes.** Full CRUD of `nd-...` nodes carrying instance type, availability
//!   zone, state DB and deterministically-derived framework endpoints (Fabric
//!   peer / Ethereum HTTP + WebSocket); `CREATING` settles to `AVAILABLE`.
//! * **Proposals + voting.** `CreateProposal` mints `p-...`, stores the invite /
//!   removal actions, sets `IN_PROGRESS` and an expiration derived from the
//!   network's `ProposalDurationInHours`. `VoteOnProposal` records a `YES`/`NO`
//!   vote per member; when the voting policy threshold is met the proposal
//!   becomes `APPROVED` (materialising each invitation into a real `Invitation`
//!   in the invited principal's account, and applying removals) or `REJECTED`.
//!   `ListProposalVotes` returns the recorded votes.
//! * **Invitations.** `ListInvitations` returns pending invitations for the
//!   account; `RejectInvitation` flips one to `REJECTED`.
//! * **Accessors.** `CreateAccessor` mints a UUID accessor id + billing token +
//!   ARN (`BILLING_TOKEN`, `AVAILABLE`); full CRUD.
//! * **Tagging.** ARN-keyed `TagResource` / `UntagResource` /
//!   `ListTagsForResource`.
//!
//! Model-driven validation rejects contract violations with the codes each
//! operation declares (`InvalidRequestException`, `ResourceNotFoundException`,
//! `IllegalActionException`).
//!
//! **Honest gap (documented, not stubbed):** Managed Blockchain does not run a
//! real Hyperledger Fabric or Ethereum network. fakecloud mirrors this: the
//! ordering-service / CA / peer / JSON-RPC endpoints it returns are well-formed,
//! deterministic URLs derived from the resource ids, but they point at no live
//! chain -- there is no orderer, CA, peer, or Ethereum RPC listening behind
//! them. The entire control plane (networks, members, nodes, proposals, votes,
//! invitations, accessors, tags) is real, account-partitioned and persisted;
//! only the blockchain data plane those endpoints would front is absent.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{ManagedBlockchainService, MANAGEDBLOCKCHAIN_ACTIONS};
pub use state::{
    ManagedBlockchainData, ManagedBlockchainSnapshot, SharedManagedBlockchainState,
    MANAGEDBLOCKCHAIN_SNAPSHOT_SCHEMA_VERSION,
};
