//! Amazon Neptune (neptune) control-plane implementation for FakeCloud.
//!
//! Neptune shares the AWS Query (form-encoded request, XML response) wire
//! protocol and API shape with Amazon RDS: it signs SigV4 with the `rds`
//! scope and talks to the `rds.<region>.amazonaws.com` endpoint. The
//! dispatcher disambiguates a real `aws-sdk-neptune` client from
//! `aws-sdk-rds` by the `api/neptune` token the SDK stamps into its
//! `user-agent`; the conformance probe signs the `neptune` scope directly.
//!
//! Honest gap: fakecloud does not run a real Neptune (Gremlin/SPARQL graph)
//! engine. Unlike the RDS crate — which boots real Postgres containers —
//! there is no Neptune engine image, so clusters and instances are
//! control-plane records with well-formed endpoints that accept no wire
//! connections. Everything else in the control plane (lifecycle, cluster
//! endpoints, snapshots, restore, parameter/subnet/global groups, IAM role
//! associations, event subscriptions, tagging) is real and persisted.

pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod validation;
pub(crate) mod xml;

pub use service::NeptuneService;
pub use state::{
    NeptuneSnapshot, NeptuneState, SharedNeptuneState, NEPTUNE_SNAPSHOT_SCHEMA_VERSION,
};
