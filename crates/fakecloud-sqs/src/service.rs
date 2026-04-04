use async_trait::async_trait;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

#[derive(Default)]
pub struct SqsService;

impl SqsService {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AwsService for SqsService {
    fn service_name(&self) -> &str {
        "sqs"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        Err(AwsServiceError::action_not_implemented(
            self.service_name(),
            &req.action,
        ))
    }

    fn supported_actions(&self) -> &[&str] {
        &[]
    }
}
