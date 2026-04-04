use async_trait::async_trait;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

#[derive(Default)]
pub struct StsService;

impl StsService {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AwsService for StsService {
    fn service_name(&self) -> &str {
        "sts"
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
