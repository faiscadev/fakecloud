use async_trait::async_trait;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::SharedKinesisState;

pub struct KinesisService {
    state: SharedKinesisState,
}

impl KinesisService {
    pub fn new(state: SharedKinesisState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for KinesisService {
    fn service_name(&self) -> &str {
        "kinesis"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _ = &self.state;
        Err(AwsServiceError::action_not_implemented(
            self.service_name(),
            &request.action,
        ))
    }

    fn supported_actions(&self) -> &[&str] {
        &[]
    }
}
