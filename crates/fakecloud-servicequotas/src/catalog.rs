//! The quotas Service Quotas knows about: every service AWS lists and each
//! one's default quotas, as AWS publishes them.
//!
//! The data is a dump of `ListServices` and `ListAWSDefaultServiceQuotas`
//! (us-east-1) vendored as `data/quotas.json.gz` and regenerated with
//! `scripts/gen-service-quotas-catalog.py`. It is decoded once, on first use,
//! and kept sorted so a lookup by service and quota code is two binary
//! searches. Questions about the hand-maintained overlay (is this quota
//! enforceable?) are answered from the static tables without decoding, so a
//! server that enforces nothing never pays for the decode on a request path.
//!
//! What AWS does not publish through the API is a hand-maintained overlay
//! here: which quotas a fakecloud service checks ([`QuotaDef::enforceable`],
//! see [`ENFORCEABLE`]), which of those AWS scales automatically
//! ([`AUTO_SCALING`]), the documented maximum an increase request can be
//! approved for ([`QuotaDef::max_value`], see [`MAX_VALUES`]) and defaults
//! that differ by region ([`REGIONAL_DEFAULTS`]). The enforcing service reads
//! an enforceable quota's applied value through
//! [`fakecloud_core::quota::QuotaProvider`] once the user switched
//! enforcement on.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::OnceLock;

use serde::Deserialize;

/// A service that has quotas.
#[derive(Debug, Clone, Copy)]
pub struct ServiceDef {
    pub code: &'static str,
    pub name: &'static str,
}

/// The CloudWatch metric that reports a quota's usage.
#[derive(Debug, Clone, Copy)]
pub struct UsageMetric {
    pub namespace: &'static str,
    pub name: &'static str,
    /// Sorted by dimension name.
    pub dimensions: &'static [(&'static str, &'static str)],
    pub statistic: &'static str,
}

/// A quota's `QuotaPeriod`, as AWS reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Period {
    pub value: i64,
    pub unit: &'static str,
}

/// The scope of a quota that applies per resource (`QuotaContextInfo`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaContext {
    pub scope: &'static str,
    pub scope_type: Option<&'static str>,
}

/// One quota.
#[derive(Debug, Clone, Copy)]
pub struct QuotaDef {
    pub service_code: &'static str,
    pub quota_code: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub default: f64,
    pub unit: &'static str,
    pub adjustable: bool,
    /// Global quotas apply to the whole account rather than a region.
    pub global: bool,
    /// The highest value an increase request can be approved for, when AWS
    /// documents one. A request above it is not approved.
    pub max_value: Option<f64>,
    pub usage_metric: Option<UsageMetric>,
    /// The `Period` AWS reports for the quota, when it reports one.
    pub period: Option<Period>,
    pub context: Option<QuotaContext>,
    /// Whether a fakecloud service checks requests against this quota once
    /// enforcement is switched on for it.
    pub enforceable: bool,
}

pub use fakecloud_core::quota::{
    FIREHOSE_MIB_PER_SECOND, FIREHOSE_PUT_REQUESTS_PER_SECOND, FIREHOSE_RECORDS_PER_SECOND,
    FIREHOSE_SERVICE_CODE, RULES_PER_SECURITY_GROUP, SECURITY_GROUPS_PER_INTERFACE,
    VPC_SERVICE_CODE as VPC,
};

pub const EC2: &str = "ec2";
/// AWS caps security groups per interface multiplied by rules per security
/// group at this value; an increase request that would exceed it is denied.
pub const SG_RULES_PRODUCT_LIMIT: f64 = 1000.0;

/// The quotas a fakecloud service checks requests against once enforcement
/// is switched on. Every other quota is informational. Sorted by service code
/// then quota code, so [`is_enforceable`] can binary search it.
pub const ENFORCEABLE: &[(&str, &str)] = &[
    ("dynamodb", "L-F98FE922"), // Maximum number of tables
    ("ec2", "L-0263D0A3"),      // EC2-VPC Elastic IPs
    ("ec2", "L-1216C47A"),      // Running On-Demand Standard instances (vCPUs)
    ("ec2", "L-1945791B"),      // Running On-Demand Inf instances
    ("ec2", "L-34B43A08"),      // All Standard Spot Instance Requests (vCPUs)
    ("ec2", "L-3E6EC3A3"),      // VPN connections per region
    ("ec2", "L-417A185B"),      // Running On-Demand P instances
    ("ec2", "L-43DA4232"),      // Running On-Demand High Memory instances
    ("ec2", "L-7295265B"),      // Running On-Demand X instances
    ("ec2", "L-74FC7D96"),      // Running On-Demand F instances
    ("ec2", "L-DB2E81BA"),      // Running On-Demand G and VT instances
    (FIREHOSE_SERVICE_CODE, FIREHOSE_RECORDS_PER_SECOND), // Records per second per stream
    (FIREHOSE_SERVICE_CODE, FIREHOSE_PUT_REQUESTS_PER_SECOND), // Put requests per second per stream
    (FIREHOSE_SERVICE_CODE, FIREHOSE_MIB_PER_SECOND), // MiB per second per stream
    ("iam", "L-0DA4ABF3"),      // Managed policies per role
    ("iam", "L-384571C4"),      // Managed policies per group
    ("iam", "L-4019AD8B"),      // Managed policies per user
    ("iam", "L-6E65F664"),      // Instance profiles per account
    ("iam", "L-858F3967"),      // OpenId connect providers per account
    ("iam", "L-BF35879D"),      // Server certificates per account
    ("iam", "L-C07B4B0D"),      // Role trust policy length
    ("iam", "L-E95E4862"),      // Customer managed policies per account
    ("iam", "L-F4A5425F"),      // Groups per account
    ("iam", "L-F55AF5E4"),      // Users per account
    ("iam", "L-FE177D64"),      // Roles per account
    ("kms", "L-C2F1777E"),      // Customer Master Keys (CMKs)
    ("lambda", "L-2ACBD22F"),   // Function and layer storage
    ("s3", "L-DC2B2D3D"),       // General purpose buckets
    ("vpc", "L-0EA8095F"),      // Inbound or outbound rules per security group
    ("vpc", "L-1B52E74A"),      // Gateway VPC endpoints per Region
    ("vpc", "L-29B6F2EB"),      // Interface VPC endpoints per VPC
    ("vpc", "L-2AEEBF1A"),      // Rules per network ACL
    ("vpc", "L-2AFB9258"),      // Security groups per network interface
    ("vpc", "L-407747CB"),      // Subnets per VPC
    ("vpc", "L-589F43AA"),      // Route tables per VPC
    ("vpc", "L-7E9ECCDB"),      // Active VPC peering connections per VPC
    ("vpc", "L-83CA0A9D"),      // IPv4 CIDR blocks per VPC
    ("vpc", "L-93826ACB"),      // Routes per route table
    ("vpc", "L-A4707A72"),      // Internet gateways per Region
    ("vpc", "L-B4A6D682"),      // Network ACLs per VPC
    ("vpc", "L-DC9F7029"),      // Outstanding VPC peering connection requests
    ("vpc", "L-DF5E4CA3"),      // Network interfaces per Region
    ("vpc", "L-E79EC296"),      // VPC security groups per Region
    ("vpc", "L-F678F1CE"),      // VPCs per Region
    ("vpc", "L-FE5A380F"),      // NAT gateways per Availability Zone
];

/// Enforceable quotas whose capacity AWS scales automatically, so their AWS
/// default describes a starting point rather than a limit. One is enforced
/// only at an applied value the account set explicitly for the region (see
/// [`crate::settings::AccountEnforcement`]). Sorted like [`ENFORCEABLE`], so
/// [`is_auto_scaling`] can binary search it; every entry is also there.
pub const AUTO_SCALING: &[(&str, &str)] = &[
    (FIREHOSE_SERVICE_CODE, FIREHOSE_RECORDS_PER_SECOND), // Records per second per stream
    (FIREHOSE_SERVICE_CODE, FIREHOSE_PUT_REQUESTS_PER_SECOND), // Put requests per second per stream
    (FIREHOSE_SERVICE_CODE, FIREHOSE_MIB_PER_SECOND),     // MiB per second per stream
];

/// Regions where Firehose's Direct PUT throughput defaults are higher.
const FIREHOSE_HIGH_THROUGHPUT_REGIONS: &[&str] = &["us-east-1", "us-west-2", "eu-west-1"];

/// AWS defaults that differ by region, which the us-east-1 dump cannot
/// carry: `(service, quota, regions, value in those regions, value
/// elsewhere)`. Every other quota's default is the same in every region.
pub const REGIONAL_DEFAULTS: &[(&str, &str, &[&str], f64, f64)] = &[
    (
        FIREHOSE_SERVICE_CODE,
        FIREHOSE_PUT_REQUESTS_PER_SECOND,
        FIREHOSE_HIGH_THROUGHPUT_REGIONS,
        2000.0,
        1000.0,
    ),
    (
        FIREHOSE_SERVICE_CODE,
        FIREHOSE_RECORDS_PER_SECOND,
        FIREHOSE_HIGH_THROUGHPUT_REGIONS,
        500_000.0,
        100_000.0,
    ),
    (
        FIREHOSE_SERVICE_CODE,
        FIREHOSE_MIB_PER_SECOND,
        FIREHOSE_HIGH_THROUGHPUT_REGIONS,
        5.0,
        1.0,
    ),
];

/// Maximum values AWS documents for adjustable quotas (Amazon VPC quotas,
/// IAM and AWS STS quotas). An increase request above one is not approved.
pub const MAX_VALUES: &[(&str, &str, f64)] = &[
    (VPC, SECURITY_GROUPS_PER_INTERFACE, 16.0),
    (VPC, "L-93826ACB", 1000.0),    // Routes per route table
    (VPC, "L-2AEEBF1A", 40.0),      // Rules per network ACL
    (VPC, "L-83CA0A9D", 50.0),      // IPv4 CIDR blocks per VPC
    (VPC, "L-085A6257", 50.0),      // IPv6 CIDR blocks per VPC
    (VPC, "L-7E9ECCDB", 125.0),     // Active VPC peering connections per VPC
    (VPC, "L-BB24F6E5", 256000.0),  // Network Address Usage
    (VPC, "L-CD17FD4B", 512000.0),  // Peered Network Address Usage
    ("iam", "L-FE177D64", 10000.0), // Roles per account
    ("iam", "L-F4A5425F", 500.0),   // Groups per account
    ("iam", "L-0DA4ABF3", 25.0),    // Managed policies per role
    ("iam", "L-4019AD8B", 20.0),    // Managed policies per user
    ("iam", "L-384571C4", 10.0),    // Managed policies per group
    ("iam", "L-E95E4862", 10000.0), // Customer managed policies per account
    ("iam", "L-BF35879D", 20.0),    // Server certificates per account
    ("iam", "L-858F3967", 700.0),   // OpenId connect providers per account
    ("iam", "L-6E65F664", 10000.0), // Instance profiles per account
    ("iam", "L-C07B4B0D", 8192.0),  // Role trust policy length
];

/// The vendored AWS dump, see `scripts/gen-service-quotas-catalog.py`.
const DATA_GZ: &[u8] = include_bytes!("../data/quotas.json.gz");

#[derive(Deserialize)]
struct RawCatalog {
    services: Vec<RawService>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RawService {
    service_code: String,
    service_name: String,
    quotas: Vec<RawQuota>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RawQuota {
    quota_code: String,
    quota_name: String,
    #[serde(default)]
    description: String,
    value: f64,
    unit: String,
    adjustable: bool,
    global_quota: bool,
    #[serde(default)]
    period: Option<RawPeriod>,
    #[serde(default)]
    usage_metric: Option<RawMetric>,
    #[serde(default)]
    quota_context: Option<RawContext>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RawPeriod {
    period_value: i64,
    period_unit: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RawMetric {
    metric_namespace: String,
    metric_name: String,
    #[serde(default)]
    metric_dimensions: BTreeMap<String, String>,
    metric_statistic_recommendation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RawContext {
    context_scope: String,
    #[serde(default)]
    context_scope_type: Option<String>,
}

struct Catalog {
    /// Ordered by service code.
    services: Vec<ServiceDef>,
    /// Each service's quotas' positions in [`Catalog::quotas`], parallel to
    /// `services`.
    ranges: Vec<Range<usize>>,
    /// Ordered by service code, then quota code.
    quotas: Vec<QuotaDef>,
}

/// Strings live as long as the catalog, which lives as long as the process.
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// Interned copies of the strings many quotas share (units, period units,
/// namespaces, statistics, dimension names and values).
#[derive(Default)]
struct Interner(HashMap<String, &'static str>);

impl Interner {
    fn get(&mut self, s: String) -> &'static str {
        if let Some(v) = self.0.get(&s) {
            return v;
        }
        let v = leak(s.clone());
        self.0.insert(s, v);
        v
    }
}

fn decode() -> Catalog {
    let mut raw: RawCatalog =
        fakecloud_core::embedded::decode_gz_json(DATA_GZ, "Service Quotas catalog");
    raw.services
        .sort_by(|a, b| a.service_code.cmp(&b.service_code));

    let mut intern = Interner::default();
    let mut services = Vec::with_capacity(raw.services.len());
    let mut ranges = Vec::with_capacity(raw.services.len());
    let mut quotas = Vec::new();
    for mut s in raw.services {
        s.quotas.sort_by(|a, b| a.quota_code.cmp(&b.quota_code));
        let code = leak(s.service_code);
        let start = quotas.len();
        for q in s.quotas {
            let quota_code = leak(q.quota_code);
            quotas.push(QuotaDef {
                service_code: code,
                quota_code,
                name: leak(q.quota_name),
                description: leak(q.description),
                default: q.value,
                unit: intern.get(q.unit),
                adjustable: q.adjustable,
                global: q.global_quota,
                max_value: None,
                usage_metric: q.usage_metric.map(|m| UsageMetric {
                    namespace: intern.get(m.metric_namespace),
                    name: intern.get(m.metric_name),
                    dimensions: Box::leak(
                        m.metric_dimensions
                            .into_iter()
                            .map(|(k, v)| (intern.get(k), intern.get(v)))
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    ),
                    statistic: intern.get(m.metric_statistic_recommendation),
                }),
                period: q.period.map(|p| Period {
                    value: p.period_value,
                    unit: intern.get(p.period_unit),
                }),
                context: q.quota_context.map(|c| QuotaContext {
                    scope: intern.get(c.context_scope),
                    scope_type: c.context_scope_type.map(|t| intern.get(t)),
                }),
                enforceable: false,
            });
        }
        ranges.push(start..quotas.len());
        services.push(ServiceDef {
            code,
            name: leak(s.service_name),
        });
    }
    let mut catalog = Catalog {
        services,
        ranges,
        quotas,
    };
    // The overlay is short; look each entry up in the sorted data.
    for &(svc, code) in ENFORCEABLE {
        if let Some(i) = catalog.index(svc, code) {
            catalog.quotas[i].enforceable = true;
        }
    }
    for &(svc, code, max) in MAX_VALUES {
        if let Some(i) = catalog.index(svc, code) {
            catalog.quotas[i].max_value = Some(max);
        }
    }
    catalog
}

impl Catalog {
    fn service_index(&self, code: &str) -> Option<usize> {
        self.services.binary_search_by(|s| s.code.cmp(code)).ok()
    }

    fn quotas_of(&self, service: usize) -> &[QuotaDef] {
        &self.quotas[self.ranges[service].clone()]
    }

    /// Position of a quota in [`Catalog::quotas`].
    fn index(&self, service_code: &str, quota_code: &str) -> Option<usize> {
        let service = self.service_index(service_code)?;
        let range = &self.ranges[service];
        self.quotas[range.clone()]
            .binary_search_by(|q| q.quota_code.cmp(quota_code))
            .ok()
            .map(|i| range.start + i)
    }
}

#[cfg(test)]
thread_local! {
    /// How many catalog reads this thread made, so a test can prove a code
    /// path never touches (and so never decodes) the catalog.
    static CATALOG_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn catalog_reads() -> usize {
    CATALOG_READS.with(|c| c.get())
}

/// Whether a fakecloud service checks `service_code/quota_code` (the
/// [`ENFORCEABLE`] overlay). Answered without decoding the catalog.
pub fn is_enforceable(service_code: &str, quota_code: &str) -> bool {
    ENFORCEABLE
        .binary_search_by(|&(s, q)| s.cmp(service_code).then_with(|| q.cmp(quota_code)))
        .is_ok()
}

/// Whether AWS scales `service_code/quota_code` automatically (the
/// [`AUTO_SCALING`] overlay). Answered without decoding the catalog.
pub fn is_auto_scaling(service_code: &str, quota_code: &str) -> bool {
    AUTO_SCALING
        .binary_search_by(|&(s, q)| s.cmp(service_code).then_with(|| q.cmp(quota_code)))
        .is_ok()
}

fn catalog() -> &'static Catalog {
    #[cfg(test)]
    CATALOG_READS.with(|c| c.set(c.get() + 1));
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(decode)
}

/// Every service, ordered by service code.
pub fn services() -> &'static [ServiceDef] {
    &catalog().services
}

/// Every quota, ordered by service code and then quota code.
pub fn quotas() -> &'static [QuotaDef] {
    &catalog().quotas
}

/// Parse a `service_code/quota_code` reference (as the CLI flags and the
/// introspection API take it) into its catalog entry.
pub fn parse_ref(reference: &str) -> Option<&'static QuotaDef> {
    let (service_code, quota_code) = reference.split_once('/')?;
    quota(service_code, quota_code)
}

pub fn service(code: &str) -> Option<&'static ServiceDef> {
    let c = catalog();
    c.service_index(code).map(|i| &c.services[i])
}

pub fn quota(service_code: &str, quota_code: &str) -> Option<&'static QuotaDef> {
    let c = catalog();
    c.index(service_code, quota_code).map(|i| &c.quotas[i])
}

/// AWS default in the requested region: the [`REGIONAL_DEFAULTS`] entry for
/// the quota, else the vendored us-east-1 value.
pub fn default_value(region: &str, def: &QuotaDef) -> f64 {
    REGIONAL_DEFAULTS
        .iter()
        .find(|&&(s, q, ..)| s == def.service_code && q == def.quota_code)
        .map_or(def.default, |&(_, _, regions, inside, elsewhere)| {
            if regions.contains(&region) {
                inside
            } else {
                elsewhere
            }
        })
}

/// Every quota of `service_code`, ordered by quota code. Empty for an
/// unknown service.
pub fn quotas_of(service_code: &str) -> &'static [QuotaDef] {
    let c = catalog();
    c.service_index(service_code)
        .map(|i| c.quotas_of(i))
        .unwrap_or(&[])
}

pub fn service_name(code: &str) -> &'static str {
    service(code).map(|s| s.name).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn firehose_defaults_follow_the_requested_region() {
        for (code, high, low) in [
            ("L-7681C790", 2000.0, 1000.0),
            ("L-17D4EADE", 500_000.0, 100_000.0),
            ("L-937C879B", 5.0, 1.0),
        ] {
            let def = quota("firehose", code).unwrap();
            for (region, expected) in [
                ("us-east-1", high),
                ("us-west-2", high),
                ("eu-west-1", high),
                ("us-east-2", low),
                ("ap-southeast-2", low),
            ] {
                assert_eq!(default_value(region, def), expected, "{region}/{code}");
            }
            assert!(!def.adjustable);
            assert!(!def.global);
            assert!(def.enforceable);
            assert!(is_enforceable("firehose", code));
        }
        // Do not regionalize other Firehose quotas or unrelated services.
        for (service, code, expected) in [
            ("firehose", "L-14BB0BE7", 5000.0),
            ("firehose", "L-040D05FF", 5.0),
            ("vpc", "L-F678F1CE", 5.0),
        ] {
            for region in ["us-east-1", "us-west-2", "eu-west-1", "us-east-2"] {
                assert_eq!(
                    default_value(region, quota(service, code).unwrap()),
                    expected
                );
            }
        }
        // A matching quota code in another service must keep its own default.
        let mut other = *quota("firehose", "L-937C879B").unwrap();
        other.service_code = "other";
        other.default = 42.0;
        assert_eq!(default_value("us-east-2", &other), 42.0);
    }

    #[test]
    fn regional_defaults_name_real_quotas_once_and_resolve_by_region() {
        let mut seen = HashSet::new();
        for &(service, code, regions, inside, elsewhere) in REGIONAL_DEFAULTS {
            let def = quota(service, code)
                .unwrap_or_else(|| panic!("regional default {service}/{code} missing"));
            assert!(seen.insert((service, code)), "duplicate {service}/{code}");
            assert!(!def.global, "{service}/{code} is global");
            assert!(!regions.is_empty(), "{service}/{code}");
            for region in regions.iter() {
                assert_eq!(default_value(region, def), inside, "{region}/{code}");
            }
            for region in ["ap-southeast-2", "us-gov-west-1", "us-east-2"] {
                assert!(!regions.contains(&region));
                assert_eq!(default_value(region, def), elsewhere, "{region}/{code}");
            }
            // The vendored dump is us-east-1's view.
            assert_eq!(def.default, default_value("us-east-1", def), "{code}");
        }
        assert_eq!(
            REGIONAL_DEFAULTS
                .iter()
                .map(|&(s, q, r, inside, elsewhere)| (s, q, r.to_vec(), inside, elsewhere))
                .collect::<Vec<_>>(),
            [
                ("L-7681C790", 2000.0, 1000.0),
                ("L-17D4EADE", 500_000.0, 100_000.0),
                ("L-937C879B", 5.0, 1.0),
            ]
            .map(|(code, inside, elsewhere)| (
                "firehose",
                code,
                vec!["us-east-1", "us-west-2", "eu-west-1"],
                inside,
                elsewhere
            ))
        );
        // Every other quota keeps its vendored default everywhere.
        for def in quotas() {
            if !seen.contains(&(def.service_code, def.quota_code)) {
                assert_eq!(default_value("ap-southeast-2", def), def.default);
            }
        }
    }

    #[test]
    fn auto_scaling_quotas_are_sorted_enforceable_and_answered_without_decoding() {
        assert!(
            AUTO_SCALING.windows(2).all(|w| w[0] < w[1]),
            "AUTO_SCALING must be sorted"
        );
        let before = catalog_reads();
        for &(svc, code) in AUTO_SCALING {
            assert!(is_enforceable(svc, code), "{svc}/{code} not enforceable");
            assert!(is_auto_scaling(svc, code), "{svc}/{code}");
        }
        assert!(!is_auto_scaling(VPC, "L-F678F1CE"));
        assert!(!is_auto_scaling("firehose", "L-14BB0BE7"));
        assert!(!is_auto_scaling("other", FIREHOSE_MIB_PER_SECOND));
        assert_eq!(catalog_reads(), before);
        assert_eq!(
            AUTO_SCALING,
            [
                ("firehose", "L-17D4EADE"),
                ("firehose", "L-7681C790"),
                ("firehose", "L-937C879B"),
            ]
        );
    }

    #[test]
    fn the_vendored_dump_decodes_into_every_listed_service() {
        let services = services();
        // Sanity floors: AWS lists over 300 services and over 14,000 quotas.
        assert!(services.len() > 300, "{}", services.len());
        assert!(quotas().len() > 14_000, "{}", quotas().len());
        assert!(services.windows(2).all(|w| w[0].code < w[1].code));
        for s in services {
            assert!(!s.name.is_empty(), "{}", s.code);
            assert_eq!(service(s.code).unwrap().name, s.name);
        }
        // A service AWS lists with no default quotas is still a service.
        assert!(service("health-agent").is_some());
        assert!(quotas_of("health-agent").is_empty());
        assert!(service("no-such-service").is_none());
        assert!(quotas_of("no-such-service").is_empty());
    }

    #[test]
    fn every_quota_belongs_to_a_listed_service_and_is_unique() {
        let mut seen = HashSet::new();
        for q in quotas() {
            assert!(service(q.service_code).is_some(), "{}", q.quota_code);
            assert!(
                seen.insert((q.service_code, q.quota_code)),
                "duplicate {}/{}",
                q.service_code,
                q.quota_code
            );
            // The index resolves every quota to itself.
            assert!(std::ptr::eq(
                quota(q.service_code, q.quota_code).unwrap(),
                q
            ));
        }
        let total: usize = services().iter().map(|s| quotas_of(s.code).len()).sum();
        assert_eq!(total, quotas().len());
        for s in services() {
            let codes: Vec<&str> = quotas_of(s.code).iter().map(|q| q.quota_code).collect();
            assert!(codes.windows(2).all(|w| w[0] < w[1]), "{} order", s.code);
        }
    }

    #[test]
    fn dump_values_spot_checks() {
        let vpcs = quota(VPC, "L-F678F1CE").unwrap();
        assert_eq!(vpcs.name, "VPCs per Region");
        assert_eq!(vpcs.default, 5.0);
        assert!(vpcs.adjustable && !vpcs.global);
        assert!(!vpcs.description.is_empty());

        let on_demand = quota(EC2, "L-1216C47A").unwrap();
        assert_eq!(on_demand.default, 5.0);
        let m = on_demand.usage_metric.unwrap();
        assert_eq!(
            (m.namespace, m.name, m.statistic),
            ("AWS/Usage", "ResourceCount", "Maximum")
        );
        assert_eq!(
            m.dimensions,
            [
                ("Class", "Standard/OnDemand"),
                ("Resource", "vCPU"),
                ("Service", "EC2"),
                ("Type", "Resource"),
            ]
        );

        let storage = quota("lambda", "L-2ACBD22F").unwrap();
        assert_eq!(storage.name, "Function and layer storage");
        assert_eq!(storage.default, 300.0);
        assert_eq!(storage.unit, "Gigabytes");
        assert!(storage.adjustable);

        // Global quotas: IAM, and S3's account-wide bucket quota.
        assert!(quota("iam", "L-FE177D64").unwrap().global);
        assert!(quota("s3", "L-DC2B2D3D").unwrap().global);

        // Periods and contexts come through as AWS reports them.
        assert!(quotas().iter().any(|q| q.period
            == Some(Period {
                value: 1,
                unit: "SECOND"
            })));
        assert!(quotas().iter().any(|q| q.context
            == Some(QuotaContext {
                scope: "RESOURCE",
                scope_type: Some("AWS::EC2::TransitGateway"),
            })));
        assert!(quotas().len() > quotas_of(EC2).len() && quotas_of(EC2).len() > 1000);
    }

    #[test]
    fn overlay_entries_point_at_dump_quotas() {
        for &(svc, code) in ENFORCEABLE {
            assert!(
                quota(svc, code).is_some(),
                "enforceable {svc}/{code} missing"
            );
        }
        for &(svc, code, max) in MAX_VALUES {
            let q = quota(svc, code).unwrap_or_else(|| panic!("max {svc}/{code} missing"));
            assert!(
                max >= q.default,
                "{svc}/{code}: max {max} < default {}",
                q.default
            );
            assert_eq!(q.max_value, Some(max));
        }
        let with_max = quotas().iter().filter(|q| q.max_value.is_some()).count();
        assert_eq!(with_max, MAX_VALUES.len());
    }

    /// Defaults, maximums and adjustability as AWS publishes them (the dump
    /// for defaults and adjustability; the Amazon VPC quotas and IAM and STS
    /// quotas pages for maximums).
    #[test]
    fn published_values() {
        let cases: &[(&str, &str, f64, Option<f64>, bool)] = &[
            (VPC, "L-93826ACB", 500.0, Some(1000.0), true),
            (VPC, "L-085A6257", 5.0, Some(50.0), true),
            (VPC, "L-BB24F6E5", 64000.0, Some(256000.0), true),
            (VPC, "L-CD17FD4B", 128000.0, Some(512000.0), true),
            ("iam", "L-0DA4ABF3", 20.0, Some(25.0), true),
            ("iam", "L-4019AD8B", 10.0, Some(20.0), true),
            ("iam", "L-384571C4", 10.0, Some(10.0), false),
            ("iam", "L-FE177D64", 1000.0, Some(10000.0), true),
            ("iam", "L-E95E4862", 1500.0, Some(10000.0), true),
            ("iam", "L-F4A5425F", 300.0, Some(500.0), true),
            ("iam", "L-6E65F664", 1000.0, Some(10000.0), true),
            ("iam", "L-C07B4B0D", 2048.0, Some(8192.0), true),
            ("iam", "L-BF35879D", 20.0, Some(20.0), true),
            ("iam", "L-858F3967", 100.0, Some(700.0), true),
            ("lambda", "L-2ACBD22F", 300.0, None, true),
        ];
        for &(service, code, default, max_value, adjustable) in cases {
            let q = quota(service, code).unwrap_or_else(|| panic!("{code} missing"));
            assert_eq!(q.default, default, "{code} default");
            assert_eq!(q.max_value, max_value, "{code} max");
            assert_eq!(q.adjustable, adjustable, "{code} adjustable");
        }
        assert_eq!(quota("lambda", "L-2ACBD22F").unwrap().unit, "Gigabytes");
        assert_eq!(quota("lambda", "L-B99A9384").unwrap().unit, "Count");
    }

    /// Every IAM `GetAccountSummary` quota IAM resolves through Service
    /// Quotas is a global `iam` quota here, with the same default.
    #[test]
    fn iam_summary_quotas_match_the_catalog() {
        for s in fakecloud_core::quota::IAM_SUMMARY_QUOTAS {
            let q = quota(fakecloud_core::quota::IAM_SERVICE_CODE, s.quota_code)
                .unwrap_or_else(|| panic!("{} missing", s.quota_code));
            assert!(q.global, "{}", s.quota_code);
            assert_eq!(q.default, s.default, "{}", s.summary_key);
        }
    }

    fn enforceable_codes(services: &[&str]) -> Vec<&'static str> {
        let mut codes: Vec<&str> = quotas()
            .iter()
            .filter(|q| services.contains(&q.service_code) && q.enforceable)
            .map(|q| q.quota_code)
            .collect();
        codes.sort_unstable();
        codes
    }

    /// The IAM, DynamoDB, KMS, S3 and Lambda quotas their services check.
    /// Lambda concurrency stays unenforced: cross-service invocations bypass
    /// the `Invoke` concurrency gate, so in-flight executions cannot be
    /// counted account-wide.
    #[test]
    fn service_quotas_enforced_by_iam_dynamodb_kms_s3_and_lambda() {
        let mut expected = [
            "L-F55AF5E4",
            "L-FE177D64",
            "L-F4A5425F",
            "L-0DA4ABF3",
            "L-4019AD8B",
            "L-384571C4",
            "L-E95E4862",
            "L-BF35879D",
            "L-858F3967",
            "L-6E65F664",
            "L-C07B4B0D",
            "L-2ACBD22F",
            "L-DC2B2D3D",
            "L-F98FE922",
            "L-C2F1777E",
        ];
        expected.sort_unstable();
        assert_eq!(
            enforceable_codes(&["iam", "dynamodb", "kms", "s3", "lambda"]),
            expected
        );
        assert!(!quota("lambda", "L-B99A9384").unwrap().enforceable);
    }

    /// The EC2 and VPC quotas EC2 checks, and the ones it cannot: no AWS
    /// error code is documented for egress-only internet gateways or transit
    /// gateways, and fakecloud keeps one IPv6 block per VPC.
    #[test]
    fn ec2_enforceable_quotas() {
        let on = [
            (VPC, "L-2AFB9258"),
            (VPC, "L-0EA8095F"),
            (VPC, "L-F678F1CE"),
            (VPC, "L-A4707A72"),
            (VPC, "L-407747CB"),
            (VPC, "L-E79EC296"),
            (VPC, "L-589F43AA"),
            (VPC, "L-93826ACB"),
            (VPC, "L-B4A6D682"),
            (VPC, "L-2AEEBF1A"),
            (VPC, "L-FE5A380F"),
            (VPC, "L-DF5E4CA3"),
            (VPC, "L-83CA0A9D"),
            (VPC, "L-7E9ECCDB"),
            (VPC, "L-DC9F7029"),
            (VPC, "L-1B52E74A"),
            (VPC, "L-29B6F2EB"),
            (EC2, "L-0263D0A3"),
            (EC2, "L-3E6EC3A3"),
            (EC2, "L-1216C47A"),
            (EC2, "L-74FC7D96"),
            (EC2, "L-DB2E81BA"),
            (EC2, "L-1945791B"),
            (EC2, "L-417A185B"),
            (EC2, "L-7295265B"),
            (EC2, "L-43DA4232"),
            (EC2, "L-34B43A08"),
        ];
        for (service, code) in on {
            assert!(
                quota(service, code).unwrap().enforceable,
                "{service}/{code}"
            );
        }
        for (service, code) in [
            (VPC, "L-45FE3B85"),
            (VPC, "L-085A6257"),
            (EC2, "L-A2478D36"),
        ] {
            assert!(
                !quota(service, code).unwrap().enforceable,
                "{service}/{code}"
            );
        }
        assert_eq!(enforceable_codes(&[VPC, EC2]).len(), on.len());
        // Nothing outside the overlay is enforceable.
        assert_eq!(
            quotas().iter().filter(|q| q.enforceable).count(),
            ENFORCEABLE.len()
        );
    }

    #[test]
    fn enforceable_list_is_sorted_for_binary_search() {
        assert!(
            ENFORCEABLE.windows(2).all(|w| w[0] < w[1]),
            "ENFORCEABLE must be sorted"
        );
        for &(svc, code) in ENFORCEABLE {
            assert!(is_enforceable(svc, code), "{svc}/{code}");
        }
    }

    /// The overlay answers enforceability without touching the catalog, and
    /// agrees with the decoded flag.
    #[test]
    fn enforceability_is_answered_without_decoding() {
        let before = catalog_reads();
        assert!(is_enforceable(VPC, "L-F678F1CE"));
        assert!(!is_enforceable("lambda", "L-B99A9384"));
        assert!(!is_enforceable("nope", "L-00000000"));
        assert_eq!(catalog_reads(), before);
        for q in quotas() {
            assert_eq!(q.enforceable, is_enforceable(q.service_code, q.quota_code));
        }
    }

    #[test]
    fn lookups_miss_cleanly() {
        assert!(quota(VPC, "L-00000000").is_none());
        assert!(quota("nope", "L-F678F1CE").is_none());
        // Service codes sort case-sensitively, as the binary search expects.
        assert!(service("AWSCloudMap").is_some());
        assert!(service("awscloudmap").is_none());
    }

    #[test]
    fn default_security_group_quotas_fit_the_product_limit() {
        let groups = quota(VPC, SECURITY_GROUPS_PER_INTERFACE).unwrap().default;
        let rules = quota(VPC, RULES_PER_SECURITY_GROUP).unwrap().default;
        assert_eq!(
            (groups, rules),
            (
                fakecloud_core::quota::DEFAULT_SECURITY_GROUPS_PER_INTERFACE as f64,
                fakecloud_core::quota::DEFAULT_RULES_PER_SECURITY_GROUP as f64
            )
        );
        assert_eq!((groups, rules), (5.0, 60.0));
        assert!(groups * rules <= SG_RULES_PRODUCT_LIMIT);
    }
}
