//! Amazon MSK (Managed Streaming for Apache Kafka, `kafka`) restJson1 control
//! plane for fakecloud.
//!
//! The full 59-operation Amazon MSK Smithy model: provisioned + serverless
//! clusters (with the async `CREATING` -> `ACTIVE` lifecycle, `UPDATING`
//! settle, and `DELETING` teardown), the `CreateClusterV2`/`DescribeClusterV2`
//! union shapes, the eleven `Update*` operations that each record a cluster
//! operation and mutate the cluster, broker-node synthesis (`ListNodes` /
//! `GetBootstrapBrokers`), configurations with monotonic base64
//! `ServerProperties` revisions, SCRAM secret association, cluster resource
//! policies, client VPC connections, replicators, topics (real control-plane
//! state this batch), the supported/compatible Kafka version catalogs, and
//! ARN-keyed resource tagging.
//!
//! Requests are routed to an operation by HTTP method + `@http` URI path; path
//! labels (full ARNs percent-encoded into a single segment) are captured
//! positionally and percent-decoded, and query parameters are read from the raw
//! query string so repeated multi-value keys (`tagKeys=a&tagKeys=b`) survive
//! intact. Everything is real, persisted, account-partitioned state: every
//! `Create`/`Update` is reflected by its `Describe`/`List`, every `Delete`
//! deletes, and AWS's async cluster lifecycle is modelled by returning the
//! transient state and settling on the next describe (reconciled on restart).
//!
//! The REAL Kafka-broker data plane is Docker-backed: each PROVISIONED cluster
//! gets a real single-node Apache Kafka broker container (KRaft combined mode),
//! topics are actually created/deleted/described/altered on that broker via its
//! own `/opt/kafka/bin/*.sh` tools, and `GetBootstrapBrokers` returns a
//! reachable `host:port` a real Kafka client produces and consumes through.
//! When no container runtime is available (or for serverless clusters) the topic
//! ops and bootstrap-broker synthesis degrade to the control-plane-only
//! behavior (a write is reflected by its read) derived from the cluster's broker
//! nodes -- the same response shapes either way.

pub mod builders;
pub mod cfn_provision;
pub mod persistence;
pub mod runtime;
pub(crate) mod service;
pub mod shared;
pub(crate) mod state;
mod validate;

pub use runtime::KafkaRuntime;
pub use service::{KafkaService, KAFKA_ACTIONS};
pub use state::{
    ClusterDataPlane, KafkaData, KafkaSnapshot, SharedKafkaState, KAFKA_SNAPSHOT_SCHEMA_VERSION,
};
