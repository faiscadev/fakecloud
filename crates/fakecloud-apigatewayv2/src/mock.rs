use bytes::Bytes;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::AwsResponse;

/// Returns a mock response
pub fn create_mock_response() -> AwsResponse {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json".parse().unwrap());

    AwsResponse {
        status: StatusCode::OK,
        content_type: "application/json".to_string(),
        headers,
        body: Bytes::from(br#"{"message":"This is a mock response"}"#.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mock_response() {
        let response = create_mock_response();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_type, "application/json");
        assert_eq!(
            response.body,
            Bytes::from(br#"{"message":"This is a mock response"}"#.to_vec())
        );
    }
}
