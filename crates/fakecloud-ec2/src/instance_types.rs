//! The EC2 instance type catalog behind `DescribeInstanceTypes`,
//! `DescribeInstanceTypeOfferings`, `GetInstanceTypesFromInstanceRequirements`
//! and the vCPU quotas: 1,430 types (current and previous generation).
//!
//! Vendored from the public catalog at instances.vantage.sh, which mirrors
//! `DescribeInstanceTypes`, by `scripts/generate-ec2-instance-catalog.py`. That
//! script documents where each value comes from and what the dataset does not
//! hold. An `Option` here is `None` when the value is genuinely unknown for that
//! type, and the element is then left out of the response. A type AWS adds later
//! is missing until the catalog is regenerated.

use std::collections::HashSet;
use std::io::Read;
use std::sync::OnceLock;

use flate2::read::GzDecoder;
use serde::Deserialize;

/// Instance types sold as Capacity Blocks (EC2 User Guide, "Capacity Blocks
/// for ML": supported instance types). The dataset has no such usage class.
pub(crate) const CAPACITY_BLOCK_INSTANCE_TYPES: &[&str] = &[
    "p4d.24xlarge",
    "p4de.24xlarge",
    "p5.4xlarge",
    "p5.48xlarge",
    "p5e.48xlarge",
    "p5en.48xlarge",
    "p6-b200.48xlarge",
    "trn1.32xlarge",
    "trn2.48xlarge",
];

/// One entry of the vendored catalog.
#[derive(Debug, Deserialize)]
pub(crate) struct InstanceTypeSpec {
    pub name: String,
    pub vcpus: u32,
    pub memory_mib: u64,
    pub cores: Option<u32>,
    pub threads_per_core: Option<u32>,
    pub architectures: Vec<String>,
    pub manufacturer: Option<String>,
    pub hypervisor: Option<String>,
    pub current_generation: bool,
    pub bare_metal: bool,
    pub burstable: bool,
    pub instance_storage_supported: Option<bool>,
    pub instance_storage: Option<InstanceStorage>,
    pub ebs_optimized_support: Option<String>,
    pub ebs_nvme_support: Option<String>,
    pub ebs_optimized_info: Option<EbsOptimizedInfo>,
    pub network_performance: String,
    pub max_network_interfaces: Option<u32>,
    pub ipv4_per_interface: Option<u32>,
    pub ipv6_supported: Option<bool>,
    pub ena_support: Option<String>,
    pub network_bandwidth_gbps: Option<f64>,
    pub nitro_enclaves_support: Option<String>,
    pub placement_strategies: Vec<String>,
    usage_classes: Vec<String>,
    pub virtualization_types: Vec<String>,
    pub accelerator: Option<Accelerator>,
    regions: Vec<String>,
}

/// The accelerators of a type, for `InstanceRequirements`. `None` members are
/// not in the dataset for this type.
#[derive(Debug, Deserialize)]
pub(crate) struct Accelerator {
    #[serde(rename = "type")]
    pub kind: String,
    pub count: Option<u32>,
    pub manufacturer: Option<String>,
    pub name: Option<String>,
    pub total_memory_mib: Option<u64>,
}

/// `InstanceStorageInfo`, the single disk group every EC2 type has.
#[derive(Debug, Deserialize)]
pub(crate) struct InstanceStorage {
    pub total_gb: u64,
    pub disk_count: u32,
    pub disk_size_gb: u64,
    pub disk_type: String,
    pub nvme_support: String,
}

/// `EbsOptimizedInfo`.
#[derive(Debug, Deserialize)]
pub(crate) struct EbsOptimizedInfo {
    pub baseline_bandwidth_mbps: u64,
    pub baseline_throughput_mbps: f64,
    pub baseline_iops: u64,
    pub max_bandwidth_mbps: u64,
    pub max_throughput_mbps: f64,
    pub max_iops: u64,
}

#[derive(Deserialize)]
struct RawCatalog {
    instance_types: Vec<InstanceTypeSpec>,
}

/// The vendored dataset, see `scripts/generate-ec2-instance-catalog.py`.
const DATA_GZ: &[u8] = include_bytes!("../data/instance_types.json.gz");

/// Every instance type, sorted by name (byte order), decoded once at first use.
pub(crate) fn catalog() -> &'static [InstanceTypeSpec] {
    static CATALOG: OnceLock<Vec<InstanceTypeSpec>> = OnceLock::new();
    CATALOG.get_or_init(|| {
        let mut json = String::new();
        GzDecoder::new(DATA_GZ)
            .read_to_string(&mut json)
            .expect("vendored instance type catalog is valid gzip");
        let raw: RawCatalog =
            serde_json::from_str(&json).expect("vendored instance type catalog is valid JSON");
        raw.instance_types
    })
}

/// The catalog entry of `instance_type`, `None` for a type EC2 does not offer.
pub(crate) fn lookup(instance_type: &str) -> Option<&'static InstanceTypeSpec> {
    let types = catalog();
    types
        .binary_search_by(|t| t.name.as_str().cmp(instance_type))
        .ok()
        .map(|i| &types[i])
}

/// Every Region the dataset knows, which sells at least one type.
fn dataset_regions() -> &'static HashSet<&'static str> {
    static REGIONS: OnceLock<HashSet<&'static str>> = OnceLock::new();
    REGIONS.get_or_init(|| {
        catalog()
            .iter()
            .flat_map(|t| t.regions.iter().map(String::as_str))
            .collect()
    })
}

/// The default vCPU count of `instance_type` in any Region, `None` for a type
/// EC2 does not offer anywhere.
pub(crate) fn default_vcpus(instance_type: &str) -> Option<u32> {
    lookup(instance_type).map(|t| t.vcpus)
}

fn one(value: impl ToString) -> Vec<String> {
    vec![value.to_string()]
}

fn known(value: Option<impl ToString>) -> Vec<String> {
    value.map(|v| v.to_string()).into_iter().collect()
}

impl InstanceTypeSpec {
    /// Whether `region` sells this type. The dataset has no prices for a Region
    /// it does not list at all (the China Regions, say), which says nothing about
    /// what is sold there, so such a Region is served the whole catalog rather
    /// than an empty one. `DescribeInstanceTypes`,
    /// `DescribeInstanceTypeOfferings` and
    /// `GetInstanceTypesFromInstanceRequirements` all scope through this.
    pub(crate) fn offered_in(&self, region: &str) -> bool {
        !dataset_regions().contains(region) || self.regions.iter().any(|r| r == region)
    }

    /// `SupportedUsageClasses`: the dataset's on-demand / spot, plus
    /// capacity-block for the types sold that way.
    pub(crate) fn usage_classes(&self) -> Vec<String> {
        let mut classes = self.usage_classes.clone();
        if CAPACITY_BLOCK_INSTANCE_TYPES.contains(&self.name.as_str()) {
            classes.push("capacity-block".to_string());
        }
        classes
    }

    /// The values `filter` compares against on this type, `None` when the
    /// catalog has no data behind that filter. An empty list never matches: the
    /// attribute is absent from this type, or unknown for it.
    pub(crate) fn filter_values(&self, filter: &str) -> Option<Vec<String>> {
        let ebs = self.ebs_optimized_info.as_ref();
        let storage = self.instance_storage.as_ref();
        Some(match filter {
            "instance-type" => one(&self.name),
            "bare-metal" => one(self.bare_metal),
            "burstable-performance-supported" => one(self.burstable),
            "current-generation" => one(self.current_generation),
            "hypervisor" => known(self.hypervisor.as_ref()),
            "memory-info.size-in-mib" => one(self.memory_mib),
            "nitro-enclaves-support" => known(self.nitro_enclaves_support.as_ref()),
            "processor-info.supported-architecture" => self.architectures.clone(),
            "supported-usage-class" => self.usage_classes(),
            "supported-virtualization-type" => self.virtualization_types.clone(),
            "vcpu-info.default-vcpus" => one(self.vcpus),
            "vcpu-info.default-cores" => known(self.cores),
            "vcpu-info.default-threads-per-core" => known(self.threads_per_core),
            "instance-storage-supported" => known(self.instance_storage_supported),
            "instance-storage-info.disk.count" => known(storage.map(|s| s.disk_count)),
            "instance-storage-info.disk.size-in-gb" => known(storage.map(|s| s.disk_size_gb)),
            "instance-storage-info.disk.type" => known(storage.map(|s| &s.disk_type)),
            "instance-storage-info.nvme-support" => known(storage.map(|s| &s.nvme_support)),
            "instance-storage-info.total-size-in-gb" => known(storage.map(|s| s.total_gb)),
            "ebs-info.ebs-optimized-support" => known(self.ebs_optimized_support.as_ref()),
            "ebs-info.nvme-support" => known(self.ebs_nvme_support.as_ref()),
            "ebs-info.ebs-optimized-info.baseline-bandwidth-in-mbps" => {
                known(ebs.map(|e| e.baseline_bandwidth_mbps))
            }
            "ebs-info.ebs-optimized-info.baseline-iops" => known(ebs.map(|e| e.baseline_iops)),
            "ebs-info.ebs-optimized-info.baseline-throughput-in-mbps" => {
                known(ebs.map(|e| e.baseline_throughput_mbps))
            }
            "ebs-info.ebs-optimized-info.maximum-bandwidth-in-mbps" => {
                known(ebs.map(|e| e.max_bandwidth_mbps))
            }
            "ebs-info.ebs-optimized-info.maximum-iops" => known(ebs.map(|e| e.max_iops)),
            "ebs-info.ebs-optimized-info.maximum-throughput-in-mbps" => {
                known(ebs.map(|e| e.max_throughput_mbps))
            }
            "network-info.ena-support" => known(self.ena_support.as_ref()),
            "network-info.ipv4-addresses-per-interface" => known(self.ipv4_per_interface),
            "network-info.ipv6-supported" => known(self.ipv6_supported),
            "network-info.maximum-network-interfaces" => known(self.max_network_interfaces),
            "network-info.network-performance" => one(&self.network_performance),
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str) -> &'static InstanceTypeSpec {
        lookup(name).unwrap_or_else(|| panic!("{name} is in the catalog"))
    }

    #[test]
    fn catalog_is_sorted_and_unique() {
        assert_eq!(catalog().len(), 1430);
        assert!(catalog().windows(2).all(|w| w[0].name < w[1].name));
    }

    #[test]
    fn every_entry_has_the_core_attributes() {
        for t in catalog() {
            assert!(t.vcpus > 0, "{}", t.name);
            assert!(t.memory_mib > 0, "{}", t.name);
            assert!(!t.architectures.is_empty(), "{}", t.name);
            assert!(!t.virtualization_types.is_empty(), "{}", t.name);
            // The sparse records, whose placement data is a placeholder, have
            // no strategies and no cores.
            assert_eq!(
                t.placement_strategies.is_empty(),
                t.cores.is_none(),
                "{}",
                t.name
            );
            if let (Some(cores), Some(threads)) = (t.cores, t.threads_per_core) {
                assert_eq!(cores * threads, t.vcpus, "{}", t.name);
            }
        }
    }

    #[test]
    fn published_vcpu_counts() {
        assert_eq!(default_vcpus("t2.micro"), Some(1));
        assert_eq!(default_vcpus("t3.micro"), Some(2));
        assert_eq!(default_vcpus("m5.large"), Some(2));
        assert_eq!(default_vcpus("c7i.48xlarge"), Some(192));
        assert_eq!(default_vcpus("u7i-12tb.224xlarge"), Some(896));
        assert_eq!(default_vcpus("m5.huge"), None);
    }

    #[test]
    fn graviton_type_has_its_real_values() {
        let t = spec("m7g.large");
        assert_eq!(t.memory_mib, 8192);
        assert_eq!(t.architectures, ["arm64"]);
        assert_eq!(t.manufacturer.as_deref(), Some("AWS"));
        assert_eq!(t.hypervisor.as_deref(), Some("nitro"));
        assert_eq!((t.cores, t.threads_per_core), (Some(2), Some(1)));
        assert_eq!(t.ebs_optimized_support.as_deref(), Some("default"));
        assert_eq!(t.ebs_nvme_support.as_deref(), Some("required"));
        assert_eq!(t.ena_support.as_deref(), Some("required"));
        assert_eq!(t.ebs_optimized_info.as_ref().unwrap().max_iops, 40000);
        assert_eq!(t.max_network_interfaces, Some(3));
        assert_eq!(t.ipv4_per_interface, Some(10));
        assert_eq!(t.network_performance, "Up to 12.5 Gigabit");
        assert_eq!(t.placement_strategies, ["cluster", "partition", "spread"]);
    }

    #[test]
    fn old_xen_types_are_not_reported_as_nitro() {
        let t2 = spec("t2.micro");
        assert_eq!(t2.hypervisor.as_deref(), Some("xen"));
        assert_eq!(t2.memory_mib, 1024);
        assert_eq!(t2.architectures, ["i386", "x86_64"]);
        assert!(t2.burstable);
        assert_eq!(t2.ena_support.as_deref(), Some("unsupported"));
        assert_eq!(t2.placement_strategies, ["partition", "spread"]);

        let c1 = spec("c1.medium");
        assert!(!c1.current_generation);
        assert_eq!(c1.memory_mib, 1740);
        assert_eq!(c1.virtualization_types, ["paravirtual"]);
        assert_eq!(c1.ipv6_supported, Some(false));
        let disk = c1.instance_storage.as_ref().unwrap();
        assert_eq!((disk.disk_count, disk.disk_size_gb), (1, 350));
        assert_eq!(disk.disk_type, "hdd");
    }

    #[test]
    fn a_xen_type_does_not_claim_what_the_data_cannot_tell() {
        let m4 = spec("m4.large");
        assert_eq!(m4.ebs_optimized_support, None);
        assert_eq!(m4.ena_support, None);
        assert!(m4.ebs_optimized_info.is_some());
    }

    #[test]
    fn sparse_records_leave_unknown_values_out() {
        let t = spec("p3.8xlarge");
        assert_eq!(t.vcpus, 32);
        assert_eq!(t.memory_mib, 249_856);
        assert_eq!(t.cores, None);
        assert_eq!(t.hypervisor, None);
        assert_eq!(t.instance_storage_supported, None);
        assert_eq!(t.ebs_optimized_support, None);
        assert_eq!(t.max_network_interfaces, None);
        assert!(t.placement_strategies.is_empty());
        assert_eq!(t.ena_support, None);
        assert_eq!(t.ipv6_supported, None);
        // The placeholders the review caught, not reported as facts.
        let u = spec("u-12tb1.112xlarge");
        assert_eq!(u.ena_support, None);
        assert!(u.placement_strategies.is_empty());
        assert_eq!(spec("i2.large").ena_support, None);
    }

    #[test]
    fn rows_sold_nowhere_are_not_vendored() {
        assert!(lookup("d3en.large").is_none());
        assert!(lookup("p6e-gb200.36xlarge").is_none());
    }

    #[test]
    fn a_type_is_offered_where_the_dataset_sells_it() {
        let only_gov = spec("i3p.16xlarge");
        assert!(only_gov.offered_in("us-gov-west-1"));
        assert!(!only_gov.offered_in("us-east-1"));
        // A Region the dataset does not know says nothing: whole catalog.
        assert!(only_gov.offered_in("cn-north-1"));
        assert!(catalog().iter().all(|t| t.offered_in("cn-north-1")));
        // The vCPU lookup quotas use does not depend on the Region.
        assert_eq!(default_vcpus("i3p.16xlarge"), Some(64));
    }

    #[test]
    fn a_missing_spot_price_is_not_a_claim_about_spot() {
        assert_eq!(spec("mac1.metal").usage_classes(), ["on-demand"]);
        assert_eq!(spec("m7g.large").usage_classes(), ["on-demand", "spot"]);
    }

    #[test]
    fn accelerators_come_from_the_dataset() {
        let g4 = spec("g4dn.xlarge").accelerator.as_ref().unwrap();
        assert_eq!(g4.kind, "gpu");
        assert_eq!(g4.count, Some(1));
        assert_eq!(g4.manufacturer.as_deref(), Some("nvidia"));
        assert_eq!(g4.name.as_deref(), Some("t4"));
        assert_eq!(g4.total_memory_mib, Some(16384));
        // A fractional GPU is the one device it shares.
        let g6f = spec("g6f.2xlarge").accelerator.as_ref().unwrap();
        assert_eq!(g6f.count, Some(1));
        let f2 = spec("f2.12xlarge").accelerator.as_ref().unwrap();
        assert_eq!((f2.kind.as_str(), f2.count), ("fpga", Some(2)));
        let vt1 = spec("vt1.3xlarge").accelerator.as_ref().unwrap();
        assert_eq!(vt1.kind, "media");
        let inf = spec("inf1.xlarge").accelerator.as_ref().unwrap();
        assert_eq!((inf.kind.as_str(), inf.count), ("inference", None));
        assert!(spec("m5.large").accelerator.is_none());
    }

    #[test]
    fn bare_metal_has_no_hypervisor() {
        let t = spec("mac1.metal");
        assert!(t.bare_metal);
        assert_eq!(t.hypervisor, None);
        assert_eq!(t.architectures, ["x86_64_mac"]);
        assert_eq!(spec("mac2.metal").architectures, ["arm64_mac"]);
    }

    #[test]
    fn capacity_block_types_are_in_the_catalog_and_report_the_class() {
        for name in CAPACITY_BLOCK_INSTANCE_TYPES {
            assert!(
                spec(name)
                    .usage_classes()
                    .contains(&"capacity-block".into()),
                "{name}"
            );
        }
        assert!(!spec("m5.large")
            .usage_classes()
            .contains(&"capacity-block".into()));
    }

    #[test]
    fn filter_values_expose_the_catalog() {
        let t = spec("i3.large");
        let v = |f: &str| t.filter_values(f).unwrap();
        assert_eq!(v("memory-info.size-in-mib"), ["15616"]);
        assert_eq!(v("instance-storage-supported"), ["true"]);
        assert_eq!(v("instance-storage-info.disk.type"), ["ssd"]);
        assert_eq!(v("hypervisor"), ["xen"]);
        assert_eq!(v("current-generation"), ["true"]);
        assert_eq!(t.filter_values("hibernation-supported"), None);
        assert!(spec("m5.large")
            .filter_values("instance-storage-info.disk.type")
            .unwrap()
            .is_empty());
    }
}
