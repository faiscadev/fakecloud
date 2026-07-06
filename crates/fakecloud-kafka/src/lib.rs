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
//! The REAL Kafka-broker data plane (topics created on a running Kafka
//! container, `GetBootstrapBrokers` returning a reachable endpoint) is a later
//! batch; this batch's topic ops and bootstrap-broker synthesis are real
//! control-plane behavior (a write is reflected by its read) derived from the
//! cluster's broker nodes.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{KafkaService, KAFKA_ACTIONS};
pub use state::{KafkaData, KafkaSnapshot, SharedKafkaState, KAFKA_SNAPSHOT_SCHEMA_VERSION};
