//! Amazon MQ (`mq`) restJson1 control plane for fakecloud.
//!
//! The full 25-operation Amazon MQ Smithy model: brokers (with the async
//! `CREATION_IN_PROGRESS` -> `RUNNING` lifecycle, `REBOOT_IN_PROGRESS` reboot
//! that applies staged pending changes, and `DELETION_IN_PROGRESS` teardown,
//! plus per-engine wire endpoints/console URLs for ActiveMQ and RabbitMQ),
//! configurations (base64 `Data` revisions, engine type, authentication
//! strategy), per-broker users (console access, groups, pending-vs-current
//! change staging), and ARN-keyed resource tagging. Also the static metadata
//! operations `DescribeBrokerEngineTypes` and `DescribeBrokerInstanceOptions`.
//!
//! Requests are routed to an operation by HTTP method + `@http` URI path; path
//! labels are captured positionally and query parameters are read from the raw
//! query string so repeated multi-value keys (`tagKeys=a&tagKeys=b`) survive
//! intact. Everything is real, persisted, account-partitioned state: every
//! `Create`/`Update` is reflected by its `Describe`/`List`, every `Delete`
//! deletes, and AWS's async broker lifecycle is modelled by returning the
//! transient state and settling on the next describe (with in-flight
//! transitions reconciled on restart).

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{MqService, MQ_ACTIONS};
pub use state::{MqData, MqSnapshot, SharedMqState, MQ_SNAPSHOT_SCHEMA_VERSION};
