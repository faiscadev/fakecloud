//! Amazon EC2 implementation for FakeCloud.
//!
//! EC2 speaks the `ec2Query` protocol (form-encoded requests, flattened-XML
//! responses — see [`fakecloud_aws::ec2query`]). This crate is built out across
//! many batches toward full operation parity; the foundation here provides the
//! service scaffold, shared `Filter`/pagination infrastructure, the resource
//! tagging subsystem, and the region/AZ/account-attribute describe primitives
//! that almost every SDK client calls implicitly.

pub mod cfn_provision;
pub mod defaults;
pub mod runtime;
pub mod service;
pub mod service_helpers;
pub mod state;

pub use runtime::Ec2Runtime;
pub use service::Ec2Service;
pub use state::{Ec2Snapshot, Ec2State, SharedEc2State, EC2_SNAPSHOT_SCHEMA_VERSION};

/// Shared test helpers for the in-crate handler unit tests.
#[cfg(test)]
pub(crate) mod test_support {
    use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

    /// Extract the error from a handler result (AwsResponse is not `Debug`, so
    /// `unwrap_err` cannot be used directly).
    pub(crate) fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error, got Ok"),
            Err(e) => e,
        }
    }

    /// Build a minimal query-protocol [`AwsRequest`] for handler unit tests.
    pub(crate) fn ec2_request(action: &str, query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }
}
