use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

pub fn put_model_invocation_logging_configuration(
    state: &SharedBedrockState,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let logging_config = body.get("loggingConfig").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "loggingConfig is required",
        )
    })?;

    let config = crate::state::LoggingConfig {
        cloud_watch_config: logging_config.get("cloudWatchConfig").cloned(),
        s3_config: logging_config.get("s3Config").cloned(),
        text_data_delivery_enabled: logging_config["textDataDeliveryEnabled"]
            .as_bool()
            .unwrap_or(true),
        image_data_delivery_enabled: logging_config["imageDataDeliveryEnabled"]
            .as_bool()
            .unwrap_or(true),
        embedding_data_delivery_enabled: logging_config["embeddingDataDeliveryEnabled"]
            .as_bool()
            .unwrap_or(true),
    };

    let mut s = state.write();
    s.logging_config = Some(config);

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

pub fn get_model_invocation_logging_configuration(
    state: &SharedBedrockState,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    match &s.logging_config {
        Some(config) => {
            let mut logging_config = json!({
                "textDataDeliveryEnabled": config.text_data_delivery_enabled,
                "imageDataDeliveryEnabled": config.image_data_delivery_enabled,
                "embeddingDataDeliveryEnabled": config.embedding_data_delivery_enabled,
            });
            if let Some(ref cw) = config.cloud_watch_config {
                logging_config["cloudWatchConfig"] = cw.clone();
            }
            if let Some(ref s3) = config.s3_config {
                logging_config["s3Config"] = s3.clone();
            }
            Ok(AwsResponse::ok_json(
                json!({ "loggingConfig": logging_config }),
            ))
        }
        None => Ok(AwsResponse::ok_json(json!({}))),
    }
}

pub fn delete_model_invocation_logging_configuration(
    state: &SharedBedrockState,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    s.logging_config = None;
    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}
