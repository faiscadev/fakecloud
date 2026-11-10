//! The commercial (`aws` partition) Regions, shared by every service that
//! enumerates them (Account `ListRegions`, EC2 `DescribeRegions`) so the lists
//! cannot drift apart.

/// Every commercial AWS Region and whether it is opt-in (disabled for an
/// account until enabled). Regions launched since 2019 are all opt-in.
pub const COMMERCIAL_REGIONS: &[(&str, bool)] = &[
    ("us-east-1", false),
    ("us-east-2", false),
    ("us-west-1", false),
    ("us-west-2", false),
    ("af-south-1", true),
    ("ap-east-1", true),
    ("ap-east-2", true),
    ("ap-south-1", false),
    ("ap-south-2", true),
    ("ap-northeast-1", false),
    ("ap-northeast-2", false),
    ("ap-northeast-3", false),
    ("ap-southeast-1", false),
    ("ap-southeast-2", false),
    ("ap-southeast-3", true),
    ("ap-southeast-4", true),
    ("ap-southeast-5", true),
    ("ap-southeast-6", true),
    ("ap-southeast-7", true),
    ("ca-central-1", false),
    ("ca-west-1", true),
    ("eu-central-1", false),
    ("eu-central-2", true),
    ("eu-west-1", false),
    ("eu-west-2", false),
    ("eu-west-3", false),
    ("eu-north-1", false),
    ("eu-south-1", true),
    ("eu-south-2", true),
    ("il-central-1", true),
    ("me-central-1", true),
    ("me-south-1", true),
    ("mx-central-1", true),
    ("sa-east-1", false),
];

/// The availability-zone letters a Region has (AWS's real AZ count). Where
/// the letters an account sees differ between accounts (us-west-1 exposes
/// two of a/b/c, ap-northeast-1 three of a/b/c/d), this is the union, so a
/// name any real account accepts is accepted here. Regions outside the
/// commercial list default to three zones, `a`-`c`.
pub fn availability_zone_letters(region: &str) -> &'static str {
    match region {
        "us-east-1" => "abcdef",
        "us-west-2" | "ap-northeast-2" | "ap-northeast-1" | "eu-west-2" => "abcd",
        "ca-central-1" => "abd",
        _ => "abc",
    }
}

/// Whether `region` is an opt-in commercial Region. `None` for a name that is
/// not a commercial Region.
pub fn is_opt_in(region: &str) -> Option<bool> {
    COMMERCIAL_REGIONS
        .iter()
        .find(|(r, _)| *r == region)
        .map(|(_, opt_in)| *opt_in)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regions_are_unique_and_include_the_newest() {
        let mut names: Vec<&str> = COMMERCIAL_REGIONS.iter().map(|(r, _)| *r).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), COMMERCIAL_REGIONS.len());
        for r in [
            "ap-southeast-6",
            "ap-southeast-7",
            "ap-east-2",
            "mx-central-1",
        ] {
            assert_eq!(is_opt_in(r), Some(true), "{r}");
        }
        assert_eq!(is_opt_in("us-east-1"), Some(false));
        assert_eq!(is_opt_in("us-gov-west-1"), None);
    }

    #[test]
    fn availability_zone_counts_follow_aws() {
        assert_eq!(availability_zone_letters("us-east-1").len(), 6);
        assert_eq!(availability_zone_letters("us-west-2").len(), 4);
        assert_eq!(availability_zone_letters("eu-west-2"), "abcd");
        // Unions of the letters different accounts see.
        assert_eq!(availability_zone_letters("us-west-1"), "abc");
        assert_eq!(availability_zone_letters("ap-northeast-1"), "abcd");
        for (r, _) in COMMERCIAL_REGIONS {
            let letters = availability_zone_letters(r);
            assert!(letters.len() >= 2, "{r}");
            // Handlers default to `{region}a` when no zone is given.
            assert!(letters.starts_with('a'), "{r}");
            assert!(letters.chars().all(|c| c.is_ascii_lowercase()), "{r}");
        }
    }

    #[test]
    fn covers_every_commercial_region_route53_models() {
        // Drift guard: Route 53's `VPCRegion` enum lists every Region; the
        // commercial ones (no gov/iso/China/sovereign partitions) must all
        // be here.
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../aws-models/route53.json");
        let model: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let members = model["shapes"]["com.amazonaws.route53#VPCRegion"]["members"]
            .as_object()
            .unwrap();
        for m in members.values() {
            let r = m["traits"]["smithy.api#enumValue"].as_str().unwrap();
            let other_partition = r.starts_with("us-gov-")
                || r.starts_with("us-iso")
                || r.starts_with("eu-iso")
                || r.starts_with("cn-")
                || r.starts_with("eusc-");
            if !other_partition {
                assert!(is_opt_in(r).is_some(), "commercial region {r} missing");
            }
        }
    }
}
