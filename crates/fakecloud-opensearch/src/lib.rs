//! Amazon OpenSearch Service + Amazon Elasticsearch Service (`es`) for FakeCloud.
//!
//! Elasticsearch Service (legacy, API version `2015-01-01`) and OpenSearch
//! Service (its successor, API version `2021-01-01`) share one SigV4 signing
//! scope (`es`) and one endpoint (`es.<region>.amazonaws.com`); on the wire
//! they are distinguished only by the URL path version prefix. They are
//! therefore ONE crate: a single [`OpenSearchService`] that routes by path
//! version to the correct API's operation set, backed by one shared domain
//! store (a `Domain` is one entity; the 2015 API exposes it via the
//! `ElasticsearchDomainStatus` shape, the 2021 API via the superset
//! `DomainStatus` shape).

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;
mod validation_gen;

pub use service::{OpenSearchService, ES_ACTIONS, OPENSEARCH_ACTIONS};
pub use state::{
    OpenSearchSnapshot, OpenSearchState, SharedOpenSearchState, OPENSEARCH_SNAPSHOT_SCHEMA_VERSION,
};
