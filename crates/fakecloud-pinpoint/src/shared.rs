//! Shared helpers for Amazon Pinpoint: id / ARN minting, timestamps, and the
//! channel platform table.

use rand::Rng;

/// A 32-character lowercase-hex identifier, matching the shape of a Pinpoint
/// application / campaign / segment / job id.
pub fn hex_id() -> String {
    let mut rng = rand::thread_rng();
    let mut s = String::with_capacity(32);
    for _ in 0..32 {
        let n: u8 = rng.gen_range(0..16);
        s.push(char::from_digit(n as u32, 16).unwrap());
    }
    s
}

/// Current time as an ISO-8601 UTC timestamp with millisecond precision.
pub fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// The ARN for an application.
pub fn app_arn(region: &str, account: &str, app_id: &str) -> String {
    format!("arn:aws:mobiletargeting:{region}:{account}:apps/{app_id}")
}

/// The ARN for a resource nested under an application (e.g.
/// `apps/<id>/campaigns/<cid>`).
pub fn nested_arn(region: &str, account: &str, app_id: &str, kind: &str, id: &str) -> String {
    format!("arn:aws:mobiletargeting:{region}:{account}:apps/{app_id}/{kind}/{id}")
}

/// Map a canonical channel key to its `Platform` value.
pub fn channel_platform(channel: &str) -> &'static str {
    match channel {
        "adm" => "ADM",
        "apns" => "APNS",
        "apns_sandbox" => "APNS_SANDBOX",
        "apns_voip" => "APNS_VOIP",
        "apns_voip_sandbox" => "APNS_VOIP_SANDBOX",
        "baidu" => "BAIDU",
        "email" => "EMAIL",
        "gcm" => "GCM",
        "sms" => "SMS",
        "voice" => "VOICE",
        _ => "CUSTOM",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_id_is_32_hex_chars() {
        let id = hex_id();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn now_iso_ends_with_z() {
        assert!(now_iso().ends_with('Z'));
    }

    #[test]
    fn arns_are_well_formed() {
        assert_eq!(
            app_arn("us-east-1", "000000000000", "abc"),
            "arn:aws:mobiletargeting:us-east-1:000000000000:apps/abc"
        );
        assert_eq!(
            nested_arn("us-east-1", "000000000000", "abc", "campaigns", "c1"),
            "arn:aws:mobiletargeting:us-east-1:000000000000:apps/abc/campaigns/c1"
        );
    }

    #[test]
    fn channel_platforms_map() {
        assert_eq!(channel_platform("apns_voip_sandbox"), "APNS_VOIP_SANDBOX");
        assert_eq!(channel_platform("sms"), "SMS");
    }
}
