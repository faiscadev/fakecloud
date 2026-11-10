//! What Firehose needs to know about other services' resources, answered by the
//! server without this crate depending on them. Unwired (an embedded service or
//! a unit test), existence is not checked and a domain is never reachable.

use std::sync::Arc;

/// A `DomainARN` as the search service sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchDomain {
    Absent,
    Present {
        /// Base URL of the domain's cluster, when a client can reach one.
        reachable_url: Option<String>,
    },
}

pub trait FirehoseTopology: Send + Sync {
    fn kinesis_stream_exists(&self, stream_arn: &str) -> bool;
    fn search_domain(&self, domain_arn: &str) -> SearchDomain;
}

pub(crate) type SharedTopology = Arc<parking_lot::RwLock<Option<Arc<dyn FirehoseTopology>>>>;
