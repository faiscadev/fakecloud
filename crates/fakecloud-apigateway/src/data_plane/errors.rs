//! apigateway data_plane `errors` concerns (audit-2026-05-19).

use super::*;

pub(super) fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg.into())
}

pub(super) fn bad_gateway(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_GATEWAY, "BadGatewayException", msg.into())
}

pub(super) fn unauthorized(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::UNAUTHORIZED,
        "UnauthorizedException",
        msg.into(),
    )
}

pub(super) fn forbidden(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::FORBIDDEN, "AccessDeniedException", msg.into())
}

/// `ForbiddenException` matches the wire shape AWS returns for an
/// API-key check failure (missing key / unknown key / disabled key).
pub(super) fn api_key_forbidden() -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::FORBIDDEN, "ForbiddenException", "Forbidden")
}

/// `LimitExceededException` is the wire shape AWS uses when throttle or
/// quota tripped at the data plane.
pub(super) fn limit_exceeded() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::TOO_MANY_REQUESTS,
        "LimitExceededException",
        "Limit Exceeded",
    )
}

pub(super) fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg.into())
}
