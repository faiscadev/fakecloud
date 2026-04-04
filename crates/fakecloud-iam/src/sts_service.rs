use async_trait::async_trait;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::SharedIamState;
use crate::xml_responses;

pub struct StsService {
    state: SharedIamState,
}

impl StsService {
    pub fn new(state: SharedIamState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for StsService {
    fn service_name(&self) -> &str {
        "sts"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "GetCallerIdentity" => self.get_caller_identity(&req),
            "AssumeRole" => self.assume_role(&req),
            _ => Err(AwsServiceError::action_not_implemented("sts", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &["GetCallerIdentity", "AssumeRole"]
    }
}

impl StsService {
    fn get_caller_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let arn = format!("arn:aws:iam::{}:root", state.account_id);
        let xml = xml_responses::get_caller_identity_response(
            &state.account_id,
            &arn,
            &state.account_id,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn assume_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;

        let role_session_name = req.query_params.get("RoleSessionName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleSessionName",
            )
        })?;

        let xml = xml_responses::assume_role_response(role_arn, role_session_name, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}
