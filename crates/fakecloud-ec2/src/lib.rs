//! Amazon EC2 implementation for FakeCloud.
//!
//! EC2 speaks the `ec2Query` protocol (form-encoded requests, flattened-XML
//! responses — see [`fakecloud_aws::ec2query`]). This crate is built out across
//! many batches toward full operation parity; the foundation here provides the
//! service scaffold, shared `Filter`/pagination infrastructure, the resource
//! tagging subsystem, and the region/AZ/account-attribute describe primitives
//! that almost every SDK client calls implicitly.

pub mod defaults;
pub mod runtime;
pub mod service;
pub mod service_helpers;
pub mod state;

pub use runtime::Ec2Runtime;
pub use service::Ec2Service;
pub use state::{Ec2State, SharedEc2State};
