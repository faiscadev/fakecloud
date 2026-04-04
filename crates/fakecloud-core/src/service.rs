use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use std::collections::HashMap;

/// A parsed AWS request.
#[derive(Debug)]
pub struct AwsRequest {
    pub service: String,
    pub action: String,
    pub region: String,
    pub account_id: String,
    pub request_id: String,
    pub headers: HeaderMap,
    pub query_params: HashMap<String, String>,
    pub body: Bytes,
    pub path_segments: Vec<String>,
    pub method: Method,
    /// Whether this request came via Query (form-encoded) or JSON protocol.
    pub is_query_protocol: bool,
}

/// A response from a service handler.
pub struct AwsResponse {
    pub status: StatusCode,
    pub content_type: String,
    pub body: Bytes,
    pub headers: HeaderMap,
}

impl AwsResponse {
    pub fn xml(status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            content_type: "text/xml".to_string(),
            body: body.into(),
            headers: HeaderMap::new(),
        }
    }

    pub fn json(status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            content_type: "application/x-amz-json-1.1".to_string(),
            body: body.into(),
            headers: HeaderMap::new(),
        }
    }
}

/// Error returned by service handlers.
#[derive(Debug, thiserror::Error)]
pub enum AwsServiceError {
    #[error("service not found: {service}")]
    ServiceNotFound { service: String },

    #[error("action {action} not implemented for service {service}")]
    ActionNotImplemented { service: String, action: String },

    #[error("{code}: {message}")]
    AwsError {
        status: StatusCode,
        code: String,
        message: String,
    },
}

impl AwsServiceError {
    pub fn action_not_implemented(service: &str, action: &str) -> Self {
        Self::ActionNotImplemented {
            service: service.to_string(),
            action: action.to_string(),
        }
    }

    pub fn aws_error(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::AwsError {
            status,
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::ServiceNotFound { .. } => StatusCode::BAD_REQUEST,
            Self::ActionNotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,
            Self::AwsError { status, .. } => *status,
        }
    }

    pub fn code(&self) -> &str {
        match self {
            Self::ServiceNotFound { .. } => "UnknownService",
            Self::ActionNotImplemented { .. } => "InvalidAction",
            Self::AwsError { code, .. } => code,
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::ServiceNotFound { service } => format!("service not found: {service}"),
            Self::ActionNotImplemented { service, action } => {
                format!("action {action} not implemented for service {service}")
            }
            Self::AwsError { message, .. } => message.clone(),
        }
    }
}

/// Trait that every AWS service implements.
#[async_trait]
pub trait AwsService: Send + Sync {
    /// The AWS service identifier (e.g., "sqs", "sns", "sts", "events", "ssm").
    fn service_name(&self) -> &str;

    /// Handle an incoming request.
    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError>;

    /// List of actions this service supports (for introspection).
    fn supported_actions(&self) -> &[&str];
}
