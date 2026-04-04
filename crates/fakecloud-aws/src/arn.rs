use std::fmt;

/// An Amazon Resource Name.
///
/// Format: `arn:partition:service:region:account-id:resource`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Arn {
    pub partition: String,
    pub service: String,
    pub region: String,
    pub account_id: String,
    pub resource: String,
}

impl Arn {
    pub fn new(service: &str, region: &str, account_id: &str, resource: &str) -> Self {
        Self {
            partition: "aws".to_string(),
            service: service.to_string(),
            region: region.to_string(),
            account_id: account_id.to_string(),
            resource: resource.to_string(),
        }
    }

    /// Create an ARN with no region (global services like IAM).
    pub fn global(service: &str, account_id: &str, resource: &str) -> Self {
        Self::new(service, "", account_id, resource)
    }
}

impl fmt::Display for Arn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "arn:{}:{}:{}:{}:{}",
            self.partition, self.service, self.region, self.account_id, self.resource
        )
    }
}

impl std::str::FromStr for Arn {
    type Err = ArnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(6, ':').collect();
        if parts.len() != 6 || parts[0] != "arn" {
            return Err(ArnParseError(s.to_string()));
        }
        Ok(Self {
            partition: parts[1].to_string(),
            service: parts[2].to_string(),
            region: parts[3].to_string(),
            account_id: parts[4].to_string(),
            resource: parts[5].to_string(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid ARN: {0}")]
pub struct ArnParseError(String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let arn = Arn::new("sqs", "us-east-1", "123456789012", "my-queue");
        let s = arn.to_string();
        assert_eq!(s, "arn:aws:sqs:us-east-1:123456789012:my-queue");
        assert_eq!(s.parse::<Arn>().unwrap(), arn);
    }

    #[test]
    fn global_arn() {
        let arn = Arn::global("iam", "123456789012", "user/admin");
        assert_eq!(arn.to_string(), "arn:aws:iam::123456789012:user/admin");
    }
}
