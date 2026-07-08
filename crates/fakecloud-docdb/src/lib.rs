//! Amazon DocumentDB (docdb) control-plane implementation for FakeCloud.
//!
//! DocumentDB shares the AWS Query (form-encoded request, XML response)
//! wire protocol and API shape with Amazon RDS: it signs SigV4 with the
//! `rds` scope and talks to the `rds.<region>.amazonaws.com` endpoint.
//! The dispatcher disambiguates a real `aws-sdk-docdb` client from
//! `aws-sdk-rds` by the `api/docdb` token the SDK stamps into its
//! `user-agent`; the conformance probe signs the `docdb` scope directly.
//!
//! Honest gap: fakecloud does not run a real DocumentDB (MongoDB-
//! compatible) engine. Unlike the RDS crate — which boots real Postgres
//! containers — there is no DocumentDB engine image, so clusters and
//! instances are control-plane records with well-formed endpoints that
//! accept no wire connections. Everything else in the control plane
//! (lifecycle, snapshots, restore, parameter/subnet/global groups, event
//! subscriptions, tagging) is real and persisted.

pub mod service;
pub mod state;
pub(crate) mod validation;
pub(crate) mod xml;

pub use service::DocDbService;
pub use state::{DocDbSnapshot, DocDbState, SharedDocDbState, DOCDB_SNAPSHOT_SCHEMA_VERSION};
