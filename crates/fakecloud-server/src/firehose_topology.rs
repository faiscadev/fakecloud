//! Answers Firehose's questions about Kinesis and OpenSearch resources.

use fakecloud_firehose::{FirehoseTopology, SearchDomain};
use fakecloud_kinesis::SharedKinesisState;
use fakecloud_opensearch::state::domain_by_arn;
use fakecloud_opensearch::SharedOpenSearchState;

pub struct ServerFirehoseTopology {
    kinesis: SharedKinesisState,
    opensearch: SharedOpenSearchState,
}

impl ServerFirehoseTopology {
    pub fn new(kinesis: SharedKinesisState, opensearch: SharedOpenSearchState) -> Self {
        Self {
            kinesis,
            opensearch,
        }
    }
}

impl FirehoseTopology for ServerFirehoseTopology {
    fn kinesis_stream_exists(&self, stream_arn: &str) -> bool {
        self.kinesis.read().by_arn(stream_arn).is_some_and(|state| {
            state
                .streams
                .values()
                .any(|stream| stream.stream_arn == stream_arn)
        })
    }

    fn search_domain(&self, domain_arn: &str) -> SearchDomain {
        match domain_by_arn(&self.opensearch.read(), domain_arn) {
            Some(domain) => SearchDomain::Present {
                reachable_url: domain.reachable_url(),
            },
            None => SearchDomain::Absent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";

    fn topology() -> ServerFirehoseTopology {
        ServerFirehoseTopology::new(
            Arc::new(RwLock::new(MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            ))),
            Arc::new(RwLock::new(MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            ))),
        )
    }

    fn domain(arn: &str, deleted: bool) -> fakecloud_opensearch::state::Domain {
        serde_json::from_value(serde_json::json!({
            "name": "logs",
            "domain_id": "id",
            "arn": arn,
            "engine_version": "OpenSearch_2.11",
            "created_via_es": false,
            "endpoint": "search-logs-id.us-east-1.es.amazonaws.com",
            "created": true,
            "deleted": deleted,
            "config": {},
            "tags": {},
            "created_at": "2026-01-01T00:00:00Z",
        }))
        .unwrap()
    }

    /// A domain exists whether or not a client can reach it: its synthetic
    /// endpoint must not pass for a reachable cluster.
    #[test]
    fn a_domain_is_present_without_a_reachable_url_and_a_deleted_one_is_absent() {
        let topology = topology();
        let arn = format!("arn:aws:es:{REGION}:{ACCOUNT}:domain/logs");
        assert_eq!(topology.search_domain(&arn), SearchDomain::Absent);
        for (deleted, expected) in [
            (
                false,
                SearchDomain::Present {
                    reachable_url: None,
                },
            ),
            (true, SearchDomain::Absent),
        ] {
            topology
                .opensearch
                .write()
                .get_or_create(ACCOUNT)
                .domains
                .insert("logs".into(), domain(&arn, deleted));
            assert_eq!(topology.search_domain(&arn), expected);
        }
        let other = format!("arn:aws:es:{REGION}:999999999999:domain/logs");
        assert_eq!(topology.search_domain(&other), SearchDomain::Absent);
    }

    /// Once a domain's backing cluster answers, DomainARN delivery goes there.
    #[test]
    fn a_domain_with_a_running_cluster_resolves_to_it() {
        let topology = topology();
        let arn = format!("arn:aws:es:{REGION}:{ACCOUNT}:domain/logs");
        let mut d = domain(&arn, false);
        d.data_plane = Some(fakecloud_opensearch::state::DomainDataPlane {
            container_id: "c1".into(),
            host: "127.0.0.1".into(),
            port: 49200,
        });
        topology
            .opensearch
            .write()
            .get_or_create(ACCOUNT)
            .domains
            .insert("logs".into(), d);
        assert_eq!(
            topology.search_domain(&arn),
            SearchDomain::Present {
                reachable_url: Some("http://127.0.0.1:49200".into()),
            }
        );
    }
}
