//! `StepFunctionsService` `validation` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StepFunctionsService {
    pub(super) fn test_state(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let definition = body["definition"]
            .as_str()
            .ok_or_else(|| missing("definition"))?;
        validate_definition(definition)?;
        let _role_arn = body["roleArn"].as_str().ok_or_else(|| missing("roleArn"))?;
        let input = body["input"].as_str().unwrap_or("{}").to_string();
        // Echo input back as output. Real Step Functions actually
        // simulates the state; our emulator reports SUCCEEDED so callers
        // can wire the integration test scaffolding.
        Ok(AwsResponse::ok_json(json!({
            "output": input,
            "status": "SUCCEEDED",
            "nextState": "End",
        })))
    }
}
