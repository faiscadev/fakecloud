/// Information extracted from an AWS SigV4 Authorization header.
///
/// We parse this for routing (service name) and identity (access key),
/// but never validate the actual signature.
#[derive(Debug, Clone)]
pub struct SigV4Info {
    pub access_key: String,
    pub date: String,
    pub region: String,
    pub service: String,
}

/// Parse the SigV4 Authorization header.
///
/// Format: `AWS4-HMAC-SHA256 Credential=AKID/20260101/us-east-1/sqs/aws4_request, SignedHeaders=..., Signature=...`
pub fn parse_sigv4(auth_header: &str) -> Option<SigV4Info> {
    let auth = auth_header.strip_prefix("AWS4-HMAC-SHA256 ")?;

    let credential_start = auth.find("Credential=")?;
    let credential_value = &auth[credential_start + 11..];
    let credential_end = credential_value.find(',')?;
    let credential = &credential_value[..credential_end];

    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 {
        return None;
    }

    Some(SigV4Info {
        access_key: parts[0].to_string(),
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123";
        let info = parse_sigv4(header).unwrap();
        assert_eq!(info.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(info.date, "20260101");
        assert_eq!(info.region, "us-east-1");
        assert_eq!(info.service, "sqs");
    }

    #[test]
    fn returns_none_for_invalid() {
        assert!(parse_sigv4("Bearer token123").is_none());
        assert!(parse_sigv4("").is_none());
    }
}
