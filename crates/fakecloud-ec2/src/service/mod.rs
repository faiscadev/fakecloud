//! EC2 service entrypoint: `ec2Query` dispatch over the per-account state.

mod meta;
mod tags;

use async_trait::async_trait;
use http::StatusCode;
use parking_lot::RwLock;
use std::sync::Arc;

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::SharedEc2State;

/// Every EC2 action this build implements. The conformance audit cross-checks
/// this list against the handwritten `#[test_action("ec2", …)]` tests, so an
/// action added here without a test fails the build. Grows one resource-family
/// batch at a time toward full 767-op parity.
pub const SUPPORTED_ACTIONS: &[&str] = &[
    // Tag introspection (the `tag:`/`resource-id` filter surface). The mutating
    // CreateTags/DeleteTags land in the next batch together with the L1 probe's
    // ec2Query request encoder (flattened `.N` lists + `ec2QueryName` member
    // renaming), which is required to verify input-bearing ops at Level 1.
    "DescribeTags",
    // Region / AZ / account describe primitives
    "DescribeRegions",
    "DescribeAvailabilityZones",
    "DescribeAccountAttributes",
];

/// Amazon EC2 service.
pub struct Ec2Service {
    pub(crate) state: SharedEc2State,
}

impl Ec2Service {
    /// Construct a service over a fresh, empty account-partitioned state.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MultiAccountState::new(
                "000000000000",
                "us-east-1",
                "",
            ))),
        }
    }

    /// Construct a service over a shared state handle (used by the server so
    /// persistence/snapshots can be wired in later batches).
    pub fn with_state(state: SharedEc2State) -> Self {
        Self { state }
    }
}

impl Default for Ec2Service {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AwsService for Ec2Service {
    fn service_name(&self) -> &str {
        "ec2"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match request.action.as_str() {
            "DescribeTags" => tags::describe_tags(self, &request),
            "DescribeRegions" => meta::describe_regions(self, &request),
            "DescribeAvailabilityZones" => meta::describe_availability_zones(self, &request),
            "DescribeAccountAttributes" => meta::describe_account_attributes(self, &request),
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                format!("The action {other} is not valid for this web service."),
            )),
        }
    }
}

impl Ec2Service {
    /// Render an `ec2Query` response for `action` with `body` as the inner XML.
    pub(crate) fn respond(action: &str, request_id: &str, body: &str) -> AwsResponse {
        AwsResponse::xml(
            StatusCode::OK,
            fakecloud_aws::ec2query::ec2_response(action, request_id, body),
        )
    }
}
