//! The EC2 instance type catalog behind `DescribeInstanceTypes`,
//! `DescribeInstanceTypeOfferings`, `GetInstanceTypesFromInstanceRequirements`,
//! `DescribeCapacityBlockOfferings` and the vCPU quotas.
//!
//! Vendored from EC2's own `DescribeInstanceTypes` and
//! `DescribeInstanceTypeOfferings` by `scripts/generate-ec2-instance-catalog.py`,
//! which documents how Regions are merged and what is carried over for Regions the
//! dumping account has not enabled. A type AWS adds later is missing until the
//! catalog is regenerated.
//!
//! Each entry keeps the API's `InstanceTypeInfo` as a tree of its XML element names
//! with every leaf the verbatim response text, so a response is rendered from the
//! data rather than field by field and nothing the API returned can be lost. Typed
//! questions are answered through [`InstanceTypeSpec::text`] and friends, with
//! dotted element paths.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io::BufReader;
use std::sync::OnceLock;

use flate2::read::GzDecoder;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;

/// Every `DescribeInstanceTypes` filter in the Smithy model, with the element of
/// `InstanceTypeInfo` it compares against.
pub(crate) const INSTANCE_TYPE_FILTERS: &[(&str, &str)] = &[
    ("auto-recovery-supported", "autoRecoverySupported"),
    ("bare-metal", "bareMetal"),
    (
        "burstable-performance-supported",
        "burstablePerformanceSupported",
    ),
    ("current-generation", "currentGeneration"),
    ("dedicated-hosts-supported", "dedicatedHostsSupported"),
    (
        "ebs-info.attachment-limit-type",
        "ebsInfo.attachmentLimitType",
    ),
    (
        "ebs-info.maximum-ebs-attachments",
        "ebsInfo.maximumEbsAttachments",
    ),
    (
        "ebs-info.ebs-optimized-info.baseline-bandwidth-in-mbps",
        "ebsInfo.ebsOptimizedInfo.baselineBandwidthInMbps",
    ),
    (
        "ebs-info.ebs-optimized-info.baseline-iops",
        "ebsInfo.ebsOptimizedInfo.baselineIops",
    ),
    (
        "ebs-info.ebs-optimized-info.baseline-throughput-in-mbps",
        "ebsInfo.ebsOptimizedInfo.baselineThroughputInMBps",
    ),
    (
        "ebs-info.ebs-optimized-info.maximum-bandwidth-in-mbps",
        "ebsInfo.ebsOptimizedInfo.maximumBandwidthInMbps",
    ),
    (
        "ebs-info.ebs-optimized-info.maximum-iops",
        "ebsInfo.ebsOptimizedInfo.maximumIops",
    ),
    (
        "ebs-info.ebs-optimized-info.maximum-throughput-in-mbps",
        "ebsInfo.ebsOptimizedInfo.maximumThroughputInMBps",
    ),
    (
        "ebs-info.ebs-optimized-support",
        "ebsInfo.ebsOptimizedSupport",
    ),
    ("ebs-info.encryption-support", "ebsInfo.encryptionSupport"),
    ("ebs-info.nvme-support", "ebsInfo.nvmeSupport"),
    ("free-tier-eligible", "freeTierEligible"),
    ("hibernation-supported", "hibernationSupported"),
    ("hypervisor", "hypervisor"),
    (
        "instance-storage-info.disk.count",
        "instanceStorageInfo.disks.count",
    ),
    (
        "instance-storage-info.disk.size-in-gb",
        "instanceStorageInfo.disks.sizeInGB",
    ),
    (
        "instance-storage-info.disk.type",
        "instanceStorageInfo.disks.type",
    ),
    (
        "instance-storage-info.encryption-support",
        "instanceStorageInfo.encryptionSupport",
    ),
    (
        "instance-storage-info.nvme-support",
        "instanceStorageInfo.nvmeSupport",
    ),
    (
        "instance-storage-info.total-size-in-gb",
        "instanceStorageInfo.totalSizeInGB",
    ),
    ("instance-storage-supported", "instanceStorageSupported"),
    ("instance-type", "instanceType"),
    ("memory-info.size-in-mib", "memoryInfo.sizeInMiB"),
    (
        "network-info.bandwidth-weightings",
        "networkInfo.bandwidthWeightings",
    ),
    (
        "network-info.efa-info.maximum-efa-interfaces",
        "networkInfo.efaInfo.maximumEfaInterfaces",
    ),
    ("network-info.efa-supported", "networkInfo.efaSupported"),
    ("network-info.ena-support", "networkInfo.enaSupport"),
    (
        "network-info.flexible-ena-queues-support",
        "networkInfo.flexibleEnaQueuesSupport",
    ),
    (
        "network-info.encryption-in-transit-supported",
        "networkInfo.encryptionInTransitSupported",
    ),
    (
        "network-info.ipv4-addresses-per-interface",
        "networkInfo.ipv4AddressesPerInterface",
    ),
    (
        "network-info.ipv6-addresses-per-interface",
        "networkInfo.ipv6AddressesPerInterface",
    ),
    ("network-info.ipv6-supported", "networkInfo.ipv6Supported"),
    (
        "network-info.maximum-network-cards",
        "networkInfo.maximumNetworkCards",
    ),
    (
        "network-info.maximum-network-interfaces",
        "networkInfo.maximumNetworkInterfaces",
    ),
    (
        "network-info.network-performance",
        "networkInfo.networkPerformance",
    ),
    ("nitro-enclaves-support", "nitroEnclavesSupport"),
    ("nitro-tpm-support", "nitroTpmSupport"),
    (
        "nitro-tpm-info.supported-versions",
        "nitroTpmInfo.supportedVersions",
    ),
    (
        "processor-info.supported-architecture",
        "processorInfo.supportedArchitectures",
    ),
    (
        "processor-info.sustained-clock-speed-in-ghz",
        "processorInfo.sustainedClockSpeedInGhz",
    ),
    (
        "processor-info.supported-features",
        "processorInfo.supportedFeatures",
    ),
    ("reboot-migration-support", "rebootMigrationSupport"),
    ("supported-boot-mode", "supportedBootModes"),
    ("supported-root-device-type", "supportedRootDeviceTypes"),
    ("supported-usage-class", "supportedUsageClasses"),
    (
        "supported-virtualization-type",
        "supportedVirtualizationTypes",
    ),
    ("vcpu-info.default-cores", "vCpuInfo.defaultCores"),
    (
        "vcpu-info.default-threads-per-core",
        "vCpuInfo.defaultThreadsPerCore",
    ),
    ("vcpu-info.default-vcpus", "vCpuInfo.defaultVCpus"),
    ("vcpu-info.valid-cores", "vCpuInfo.validCores"),
    (
        "vcpu-info.valid-threads-per-core",
        "vCpuInfo.validThreadsPerCore",
    ),
];

/// One entry of the vendored catalog.
#[derive(Debug, Deserialize)]
pub(crate) struct InstanceTypeSpec {
    pub name: String,
    /// `InstanceTypeInfo`, element names and verbatim text.
    info: Node,
    /// Region -> the letters of its zones selling this type. Empty when the zones
    /// are unknown: a Region the dumping account has not enabled.
    offerings: BTreeMap<String, String>,
    /// Region -> the top-level elements EC2 reports differently there than `info`,
    /// which holds the union of all Regions for a Region without its own data.
    #[serde(default)]
    overrides: BTreeMap<String, BTreeMap<String, Node>>,
}

/// The accelerators of one kind on a type, for `InstanceRequirements`.
#[derive(Debug, PartialEq)]
pub(crate) struct Accelerator {
    /// The API's `AcceleratorType`. `None` for Trainium: real EC2's
    /// `AcceleratorTypes=[inference]` lists only Inferentia, and no other value is
    /// recorded for it.
    pub kind: Option<&'static str>,
    /// What `AcceleratorCount` compares against. For a GPU, its logical GPUs times
    /// their partition size: real EC2 lists a fractional one (`count` 0, 1 logical GPU,
    /// partition 0.125 to 0.5) under `Max=1` and `Min=0`, but under neither `Max=0` nor
    /// `Min=1`, which is what a value between 0 and 1 does.
    pub count: f64,
    /// The API's `AcceleratorManufacturer`, `None` for a maker outside that enum.
    pub manufacturer: Option<&'static str>,
    /// The API's `AcceleratorName`, `None` for a device outside that enum.
    pub name: Option<&'static str>,
}

/// One device block of `InstanceTypeInfo`.
struct AcceleratorBlock {
    block: &'static str,
    devices: &'static str,
    total_memory: &'static str,
    kind: Option<&'static str>,
}

const ACCELERATOR_BLOCKS: &[AcceleratorBlock] = &[
    AcceleratorBlock {
        block: "gpuInfo",
        devices: "gpus",
        total_memory: "totalGpuMemoryInMiB",
        kind: Some("gpu"),
    },
    AcceleratorBlock {
        block: "fpgaInfo",
        devices: "fpgas",
        total_memory: "totalFpgaMemoryInMiB",
        kind: Some("fpga"),
    },
    AcceleratorBlock {
        block: "inferenceAcceleratorInfo",
        devices: "accelerators",
        total_memory: "totalInferenceMemoryInMiB",
        kind: Some("inference"),
    },
    AcceleratorBlock {
        block: "neuronInfo",
        devices: "neuronDevices",
        total_memory: "totalNeuronDeviceMemoryInMiB",
        kind: None,
    },
    AcceleratorBlock {
        block: "mediaAcceleratorInfo",
        devices: "accelerators",
        total_memory: "totalMediaMemoryInMiB",
        kind: Some("media"),
    },
];

/// The `AcceleratorName` values, in the order real EC2's refusal lists them.
pub(crate) const ACCELERATOR_NAME_VALUES: &[&str] = &[
    "a10g",
    "a100",
    "h100",
    "inferentia",
    "k520",
    "k80",
    "m60",
    "radeon-pro-v520",
    "t4",
    "t4g",
    "vu9p",
    "v100",
    "l40s",
    "l4",
    "gaudi-hl-205",
    "inferentia2",
    "trainium",
    "trainium2",
    "u30",
];

/// Every name a response gives a device -> the API's `AcceleratorName`.
const ACCELERATOR_NAMES: &[(&str, &str)] = &[
    ("A100", "a100"),
    ("A10G", "a10g"),
    ("Gaudi HL-205", "gaudi-hl-205"),
    ("H100", "h100"),
    ("Inferentia", "inferentia"),
    ("Inferentia2", "inferentia2"),
    ("K520", "k520"),
    ("K80", "k80"),
    ("L4", "l4"),
    ("L40S", "l40s"),
    ("M60", "m60"),
    ("Radeon Pro V520", "radeon-pro-v520"),
    ("T4", "t4"),
    ("T4g", "t4g"),
    ("Trainium", "trainium"),
    ("Trainium2", "trainium2"),
    ("U30", "u30"),
    ("V100", "v100"),
    ("Virtex UltraScale (VU9P)", "vu9p"),
];

/// Device names real EC2's recorded `AcceleratorName` enum has no value for, so no
/// `AcceleratorNames` request can select them.
#[cfg(test)]
const UNNAMED_ACCELERATORS: &[&str] = &[
    "B200",
    "B300",
    "H200",
    "Qualcomm AI100",
    "RTX PRO 4500",
    "RTX PRO Server 6000",
    "Virtex UltraScale+ (VU47P)",
];

/// A response's manufacturer -> the API's `AcceleratorManufacturer`. Qualcomm has no
/// value there.
fn accelerator_manufacturer(manufacturer: &str) -> Option<&'static str> {
    match manufacturer {
        "AWS" => Some("amazon-web-services"),
        "AMD" => Some("amd"),
        "Habana" => Some("habana"),
        "NVIDIA" => Some("nvidia"),
        "Xilinx" => Some("xilinx"),
        _ => None,
    }
}

/// A piece of `InstanceTypeInfo`. The catalog is a few thousand distinct strings
/// repeated across a great many leaves, so they are interned: as `serde_json::Value`
/// the first decode costs some 35 MiB of RSS, this way some 12 MiB (and some 40 ms).
#[derive(Debug)]
enum Node {
    Text(&'static str),
    List(Box<[Node]>),
    Fields(Box<[(&'static str, Node)]>),
}

impl Node {
    fn get(&self, name: &str) -> Option<&Node> {
        match self {
            Node::Fields(fields) => fields.iter().find(|(n, _)| *n == name).map(|(_, v)| v),
            _ => None,
        }
    }
}

thread_local! {
    /// The strings interned so far, only while [`raw_catalog`] decodes on this
    /// thread: it is emptied when the decode is done.
    static INTERNED: std::cell::RefCell<HashSet<&'static str>> =
        std::cell::RefCell::new(HashSet::new());
}

/// Each distinct string once. They live as long as the catalog, which is the
/// life of the process.
fn intern(text: &str) -> &'static str {
    INTERNED.with(|interned| {
        let mut interned = interned.borrow_mut();
        if let Some(known) = interned.get(text) {
            return *known;
        }
        let leaked: &'static str = Box::leak(text.to_owned().into_boxed_str());
        interned.insert(leaked);
        leaked
    })
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct NodeVisitor;
        impl<'de> Visitor<'de> for NodeVisitor {
            type Value = Node;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string, a list or a structure")
            }

            fn visit_str<E>(self, text: &str) -> Result<Node, E> {
                Ok(Node::Text(intern(text)))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Node, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Node::List(items.into_boxed_slice()))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Node, A::Error> {
                let mut fields = Vec::new();
                while let Some((name, value)) = map.next_entry::<String, Node>()? {
                    fields.push((intern(&name), value));
                }
                Ok(Node::Fields(fields.into_boxed_slice()))
            }
        }
        deserializer.deserialize_any(NodeVisitor)
    }
}

#[derive(Deserialize)]
struct RawCatalog {
    instance_types: Vec<InstanceTypeSpec>,
    /// `BaselinePerformanceFactors` reference family -> the families it includes.
    baseline_performance: HashMap<String, HashSet<String>>,
    /// The Regions dumped from EC2, as opposed to carried over.
    dump_regions: HashSet<String>,
    /// Region -> types `GetInstanceTypesFromInstanceRequirements` does not list there
    /// though it sells them. Real EC2 leaves these out whatever is asked, and does not
    /// say why.
    requirements_omitted: HashMap<String, HashSet<String>>,
    /// Region -> types its `IncludeUnsupportedInRegion` listing leaves out, so that
    /// they are unknown there even when asked for.
    not_described: HashMap<String, HashSet<String>>,
    /// Region -> types it lists there although `DescribeInstanceTypes` does not.
    requirements_extra: HashMap<String, HashSet<String>>,
}

/// The vendored dataset, see `scripts/generate-ec2-instance-catalog.py`.
const DATA_GZ: &[u8] = include_bytes!("../data/instance_types.json.gz");

/// Decoded once at first use.
fn raw_catalog() -> &'static RawCatalog {
    static CATALOG: OnceLock<RawCatalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        let json = BufReader::new(GzDecoder::new(DATA_GZ));
        let raw = serde_json::from_reader(json).expect("vendored instance type catalog is valid");
        INTERNED.with(|interned| interned.take());
        raw
    })
}

/// Every instance type, sorted by name (byte order).
pub(crate) fn catalog() -> &'static [InstanceTypeSpec] {
    &raw_catalog().instance_types
}

/// The catalog entry of `instance_type`, `None` for a type EC2 does not offer.
pub(crate) fn lookup(instance_type: &str) -> Option<&'static InstanceTypeSpec> {
    let types = catalog();
    types
        .binary_search_by(|t| t.name.as_str().cmp(instance_type))
        .ok()
        .map(|i| &types[i])
}

/// Every Region the catalog knows, which sells at least one type.
fn catalog_regions() -> &'static HashSet<&'static str> {
    static REGIONS: OnceLock<HashSet<&'static str>> = OnceLock::new();
    REGIONS.get_or_init(|| {
        catalog()
            .iter()
            .flat_map(|t| t.offerings.keys().map(String::as_str))
            .collect()
    })
}

/// Region -> the zone letters the catalog has data for.
fn known_zone_letters() -> &'static HashMap<&'static str, HashSet<char>> {
    static LETTERS: OnceLock<HashMap<&'static str, HashSet<char>>> = OnceLock::new();
    LETTERS.get_or_init(|| {
        let mut letters: HashMap<&'static str, HashSet<char>> = HashMap::new();
        for t in catalog() {
            for (region, zones) in &t.offerings {
                letters.entry(region).or_default().extend(zones.chars());
            }
        }
        letters
    })
}

/// The default vCPU count of `instance_type` in any Region, `None` for a type
/// EC2 does not offer anywhere.
pub(crate) fn default_vcpus(instance_type: &str) -> Option<u32> {
    lookup(instance_type).and_then(|t| t.number("vCpuInfo.defaultVCpus").map(|v| v as u32))
}

/// The instance types `region` sells as Capacity Blocks: what its own
/// `supportedUsageClasses` says. A Region without its own data has the union of all
/// Regions, so it can list a type it does not really sell that way.
pub(crate) fn capacity_block_types(region: &str) -> Vec<&'static str> {
    catalog()
        .iter()
        .filter(|t| t.offered_in(region))
        .filter(|t| {
            t.values_in(region, "supportedUsageClasses")
                .contains(&"capacity-block")
        })
        .map(|t| t.name.as_str())
        .collect()
}

/// Every leaf under `path`, flattening lists.
fn leaves<'a>(node: &'a Node, path: &str, out: &mut Vec<&'a str>) {
    match node {
        Node::List(items) => items.iter().for_each(|n| leaves(n, path, out)),
        Node::Fields(_) => {
            let (head, tail) = path.split_once('.').unwrap_or((path, ""));
            if let Some(next) = node.get(head) {
                leaves(next, tail, out);
            }
        }
        // An empty element holds no value. The API has one: `supportedUsageClasses` of
        // a few types no Region sells (cc1, hs1, u-*.metal).
        Node::Text(text) if path.is_empty() && !text.is_empty() => out.push(text),
        Node::Text(_) => {}
    }
}

/// `node` as the content of element `name`, a child of `parent`.
fn write_content(name: &str, parent: &str, node: &Node, out: &mut String) {
    match node {
        Node::Text(text) => out.push_str(&fakecloud_aws::xml::xml_escape(text)),
        Node::List(items) => {
            // InferenceDeviceInfoList is the one list in InstanceTypeInfo the model
            // leaves on Smithy's default entry name.
            let entry = if parent == "inferenceAcceleratorInfo" {
                "member"
            } else {
                "item"
            };
            for item in items {
                write_element(entry, name, item, out);
            }
        }
        Node::Fields(fields) => {
            for (field, content) in fields {
                write_element(field, name, content, out);
            }
        }
    }
}

fn write_element(name: &str, parent: &str, node: &Node, out: &mut String) {
    out.push('<');
    out.push_str(name);
    out.push('>');
    write_content(name, parent, node, out);
    out.push_str("</");
    out.push_str(name);
    out.push('>');
}

impl InstanceTypeSpec {
    /// Every leaf under a dotted element path, flattening lists: the values of
    /// `networkInfo.networkCards.peakBandwidthInGbps` are one per card. Empty when
    /// the type has no such element.
    pub(crate) fn values(&self, path: &str) -> Vec<&str> {
        let mut out = Vec::new();
        leaves(&self.info, path, &mut out);
        out
    }

    /// The single value at `path`.
    pub(crate) fn text(&self, path: &str) -> Option<&str> {
        self.values(path).first().copied()
    }

    pub(crate) fn number(&self, path: &str) -> Option<f64> {
        self.text(path).and_then(|v| v.parse().ok())
    }

    pub(crate) fn flag(&self, path: &str) -> bool {
        self.known_flag(path) == Some(true)
    }

    /// Like [`Self::values`], as `region` reports it.
    pub(crate) fn values_in(&self, region: &str, path: &str) -> Vec<&str> {
        let (head, tail) = path.split_once('.').unwrap_or((path, ""));
        match self.overrides.get(region).and_then(|o| o.get(head)) {
            Some(node) => {
                let mut out = Vec::new();
                leaves(node, tail, &mut out);
                out
            }
            None => self.values(path),
        }
    }

    /// `true` or `false` at `path`, `None` when the catalog does not know.
    pub(crate) fn known_flag(&self, path: &str) -> Option<bool> {
        match self.text(path) {
            Some("true") => Some(true),
            Some("false") => Some(false),
            _ => None,
        }
    }

    /// Whether EC2 itself described this type. One it no longer does keeps only basic
    /// specs from the older catalog, so the absence of a block says nothing about it.
    fn described_by_ec2(&self) -> bool {
        self.info.get("rebootMigrationSupport").is_some()
    }

    /// Whether `region` sells this type. The catalog has no data at all for a
    /// Region it does not list (the China Regions, say), which says nothing about
    /// what is sold there, so such a Region is served the whole catalog rather
    /// than an empty one. `DescribeInstanceTypes`,
    /// `DescribeInstanceTypeOfferings` and
    /// `GetInstanceTypesFromInstanceRequirements` all scope through this.
    pub(crate) fn offered_in(&self, region: &str) -> bool {
        !catalog_regions().contains(region) || self.offerings.contains_key(region)
    }

    /// Whether `IncludeUnsupportedInRegion` shows this type in `region`.
    pub(crate) fn described_with_unsupported_in(&self, region: &str) -> bool {
        !raw_catalog()
            .not_described
            .get(region)
            .is_some_and(|absent| absent.contains(&self.name))
    }

    /// Whether `GetInstanceTypesFromInstanceRequirements` can list this type in
    /// `region`. It is not quite what the Region sells, see [`RawCatalog`].
    pub(crate) fn listed_by_requirements_in(&self, region: &str) -> bool {
        let raw = raw_catalog();
        // A Region without its own data follows the ones that have it.
        let omitted = if raw.dump_regions.contains(region) {
            raw.requirements_omitted
                .get(region)
                .is_some_and(|omitted| omitted.contains(&self.name))
        } else {
            raw.requirements_omitted
                .values()
                .any(|omitted| omitted.contains(&self.name))
        };
        !omitted
            && (self.offered_in(region)
                || raw
                    .requirements_extra
                    .get(region)
                    .is_some_and(|extra| extra.contains(&self.name)))
    }

    /// Whether this type's family is included under a `BaselinePerformanceFactors`
    /// reference family, as real EC2 answered per family. A reference it does not
    /// know includes nothing, as on EC2. References are case-insensitive.
    pub(crate) fn included_by_baseline_reference(&self, reference: &str) -> bool {
        raw_catalog()
            .baseline_performance
            .get(&reference.to_ascii_lowercase())
            .is_some_and(|included| included.contains(self.family()))
    }

    /// `m5` of `m5.large`.
    pub(crate) fn family(&self) -> &str {
        self.name.split('.').next().unwrap_or_default()
    }

    /// The zones of `region` selling this type.
    pub(crate) fn zones_selling(&self, region: &str) -> Vec<String> {
        crate::defaults::region_zones(region)
            .into_iter()
            .map(|(zone, _)| zone)
            .filter(|zone| self.offered_in_zone(region, zone))
            .collect()
    }

    /// Whether `zone` of `region` sells this type. A zone the catalog has no data
    /// for (a Region the dumping account has not enabled, or a letter another
    /// account has but that one does not) sells what its Region sells.
    pub(crate) fn offered_in_zone(&self, region: &str, zone: &str) -> bool {
        if !self.offered_in(region) {
            return false;
        }
        let Some(letter) = zone.strip_prefix(region).and_then(|l| l.chars().next()) else {
            return false;
        };
        let known = known_zone_letters()
            .get(region)
            .is_some_and(|letters| letters.contains(&letter));
        !known
            || self
                .offerings
                .get(region)
                .is_none_or(|zones| zones.is_empty() || zones.contains(letter))
    }

    /// Everything of `InstanceTypeInfo` except `supportedInRegion`, as `region`
    /// reports it, as XML.
    pub(crate) fn info_xml(&self, region: &str) -> String {
        let mut out = String::with_capacity(4096);
        let overrides = self.overrides.get(region);
        if let Node::Fields(fields) = &self.info {
            for (name, node) in fields {
                let node = overrides.and_then(|o| o.get(*name)).unwrap_or(node);
                write_element(name, "", node, &mut out);
            }
        }
        out
    }

    /// The baseline network bandwidth of the whole type, in Gbps: the sum over its
    /// network cards, as `NetworkPerformance` reports it for multi-card types.
    pub(crate) fn baseline_network_gbps(&self) -> Option<f64> {
        let cards = self.values("networkInfo.networkCards.baselineBandwidthInGbps");
        if cards.is_empty() {
            return None;
        }
        Some(cards.iter().filter_map(|v| v.parse::<f64>().ok()).sum())
    }

    /// The device blocks that count. An Inferentia type lists its chips in both
    /// `inferenceAcceleratorInfo` and `neuronInfo`, so a Neuron block only counts on a
    /// type without the former (Trainium).
    fn accelerator_blocks(&self) -> impl Iterator<Item = (&AcceleratorBlock, &Node)> {
        let has_inference = self.info.get("inferenceAcceleratorInfo").is_some();
        ACCELERATOR_BLOCKS
            .iter()
            .filter(move |b| !(has_inference && b.block == "neuronInfo"))
            .filter_map(|b| self.info.get(b.block).map(|node| (b, node)))
    }

    /// The accelerators this type carries, `None` when the catalog does not know.
    pub(crate) fn accelerators(&self) -> Option<Vec<Accelerator>> {
        if !self.described_by_ec2() {
            return None;
        }
        let mut found = Vec::new();
        for (block, node) in self.accelerator_blocks() {
            let Some(Node::List(devices)) = node.get(block.devices) else {
                continue;
            };
            for device in devices {
                let field = |name: &str| match device.get(name) {
                    Some(Node::Text(text)) => Some(*text),
                    _ => None,
                };
                let number = |name: &str| field(name).and_then(|c| c.parse::<f64>().ok());
                let partitioned = number("logicalGpuCount")
                    .zip(number("gpuPartitionSize"))
                    .map(|(logical, size)| logical * size);
                let name = field("name").unwrap_or_default();
                found.push(Accelerator {
                    kind: block.kind,
                    count: partitioned.or(number("count")).unwrap_or(0.0),
                    // Neuron devices have no manufacturer element: they are AWS's.
                    manufacturer: accelerator_manufacturer(field("manufacturer").unwrap_or("AWS")),
                    name: ACCELERATOR_NAMES
                        .iter()
                        .find(|(response, _)| *response == name)
                        .map(|(_, api)| *api),
                });
            }
        }
        Some(found)
    }

    /// The accelerators' total memory, `None` when the catalog does not know.
    pub(crate) fn accelerator_memory_mib(&self) -> Option<f64> {
        self.described_by_ec2().then(|| {
            self.accelerator_blocks()
                .filter_map(|(block, node)| match node.get(block.total_memory) {
                    Some(Node::Text(text)) => text.parse::<f64>().ok(),
                    _ => None,
                })
                .sum()
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    fn spec(name: &str) -> &'static InstanceTypeSpec {
        lookup(name).unwrap_or_else(|| panic!("{name} is in the catalog"))
    }

    /// Records straight out of the EC2 dumps. A list of plain values compares
    /// unordered: Regions order some of them differently and the catalog keeps the
    /// first Region's order.
    fn matches_dump(path: &str, dump: &Value, node: &Node) {
        match (dump, node) {
            (Value::Object(expected), Node::Fields(fields)) => {
                let mut names: Vec<&str> = fields.iter().map(|(n, _)| *n).collect();
                names.sort_unstable();
                let mut wanted: Vec<&str> = expected.keys().map(String::as_str).collect();
                wanted.sort_unstable();
                assert_eq!(wanted, names, "{path}");
                for (name, value) in expected {
                    matches_dump(&format!("{path}.{name}"), value, node.get(name).unwrap());
                }
            }
            (Value::Array(expected), Node::List(items)) => {
                assert_eq!(expected.len(), items.len(), "{path}");
                let text = |n: &Node| match n {
                    Node::Text(t) => Some(t.to_string()),
                    _ => None,
                };
                if expected.iter().all(Value::is_string) {
                    let mut wanted: Vec<&str> = expected.iter().flat_map(Value::as_str).collect();
                    wanted.sort_unstable();
                    let mut got: Vec<String> = items.iter().flat_map(text).collect();
                    got.sort_unstable();
                    assert_eq!(wanted, got, "{path}");
                } else {
                    for (value, item) in expected.iter().zip(items.iter()) {
                        matches_dump(path, value, item);
                    }
                }
            }
            (Value::String(expected), Node::Text(text)) => assert_eq!(expected, text, "{path}"),
            _ => panic!("{path}: {dump:?} against {node:?}"),
        }
    }

    fn goldens() -> BTreeMap<String, Value> {
        serde_json::from_str(include_str!("testdata/instance_type_goldens.json")).unwrap()
    }

    #[test]
    fn goldens_match_the_ec2_dump_field_by_field() {
        let goldens = goldens();
        assert!(goldens.len() >= 5);
        for (name, golden) in &goldens {
            // The union of Regions is the one thing the catalog adds to a Region's
            // own record.
            matches_dump(name, golden, &spec(name).info);
        }
    }

    #[test]
    fn rendering_covers_every_leaf_of_a_golden() {
        fn check(xml: &str, value: &Value) {
            match value {
                Value::Object(fields) => {
                    for (k, v) in fields {
                        assert!(xml.contains(&format!("<{k}>")), "{k}");
                        check(xml, v);
                    }
                }
                Value::Array(items) => items.iter().for_each(|v| check(xml, v)),
                Value::String(text) => assert!(xml.contains(&format!(">{text}<")), "{text}"),
                _ => panic!("the catalog holds only text leaves"),
            }
        }
        for (name, golden) in &goldens() {
            check(&spec(name).info_xml("us-east-1"), golden);
        }
    }

    /// EC2's own numbers, not derivations: fields the catalog could not have before.
    #[test]
    fn m5_large_carries_what_only_the_api_has() {
        let t = spec("m5.large");
        assert_eq!(t.text("ebsInfo.maximumEbsAttachments"), Some("27"));
        assert_eq!(t.text("ebsInfo.attachmentLimitType"), Some("shared"));
        assert_eq!(t.text("rebootMigrationSupport"), Some("supported"));
        assert_eq!(t.values("vCpuInfo.validThreadsPerCore"), ["1", "2"]);
        assert_eq!(t.values("supportedBootModes"), ["legacy-bios", "uefi"]);
        assert_eq!(
            t.text("networkInfo.networkCards.peakBandwidthInGbps"),
            Some("10.0")
        );
        assert_eq!(t.baseline_network_gbps(), Some(0.75));
        // Verbatim text, 1250.0 stays 1250.0.
        assert_eq!(
            t.text("ebsInfo.ebsOptimizedInfo.maximumThroughputInMBps"),
            Some("593.75")
        );
    }

    #[test]
    fn catalog_is_sorted_and_unique() {
        assert_eq!(catalog().len(), 1437);
        assert!(catalog().windows(2).all(|w| w[0].name < w[1].name));
    }

    #[test]
    fn every_entry_has_the_core_attributes() {
        for t in catalog() {
            assert_eq!(t.text("instanceType"), Some(t.name.as_str()));
            assert!(
                t.number("vCpuInfo.defaultVCpus").unwrap() > 0.0,
                "{}",
                t.name
            );
            assert!(
                t.number("memoryInfo.sizeInMiB").unwrap() > 0.0,
                "{}",
                t.name
            );
            assert!(
                !t.values("processorInfo.supportedArchitectures").is_empty(),
                "{}",
                t.name
            );
            assert!(t.info.get("ebsInfo").is_some(), "{}", t.name);
            assert!(t.info.get("networkInfo").is_some(), "{}", t.name);
            assert!(t.info.get("placementGroupInfo").is_some(), "{}", t.name);
            assert!(t.info.get("supportedInRegion").is_none(), "{}", t.name);
        }
    }

    /// Why no filter needs refusing: each has real values to compare against.
    #[test]
    fn every_filter_has_data_on_some_type() {
        assert_eq!(INSTANCE_TYPE_FILTERS.len(), 56);
        for (filter, path) in INSTANCE_TYPE_FILTERS {
            assert!(
                catalog().iter().any(|t| !t.values(path).is_empty()),
                "{filter} -> {path}"
            );
        }
    }

    #[test]
    fn published_vcpu_counts() {
        assert_eq!(default_vcpus("t2.micro"), Some(1));
        assert_eq!(default_vcpus("t3.micro"), Some(2));
        assert_eq!(default_vcpus("m5.large"), Some(2));
        assert_eq!(default_vcpus("c7i.48xlarge"), Some(192));
        assert_eq!(default_vcpus("u7i-12tb.224xlarge"), Some(896));
        // Sold nowhere the dumps reach, still known.
        assert_eq!(default_vcpus("dl1.24xlarge"), Some(96));
        assert_eq!(default_vcpus("m5.huge"), None);
    }

    #[test]
    fn graviton_type_has_its_real_values() {
        let t = spec("m7g.large");
        assert_eq!(t.number("memoryInfo.sizeInMiB"), Some(8192.0));
        assert_eq!(t.values("processorInfo.supportedArchitectures"), ["arm64"]);
        assert_eq!(t.text("processorInfo.manufacturer"), Some("AWS"));
        assert_eq!(t.text("hypervisor"), Some("nitro"));
        assert_eq!(t.text("vCpuInfo.defaultCores"), Some("2"));
        assert_eq!(t.text("vCpuInfo.defaultThreadsPerCore"), Some("1"));
        assert_eq!(t.text("ebsInfo.ebsOptimizedSupport"), Some("default"));
        assert_eq!(t.text("ebsInfo.nvmeSupport"), Some("required"));
        assert_eq!(t.text("networkInfo.enaSupport"), Some("required"));
        assert_eq!(
            t.text("ebsInfo.ebsOptimizedInfo.maximumIops"),
            Some("40000")
        );
        assert_eq!(t.text("networkInfo.maximumNetworkInterfaces"), Some("3"));
        assert_eq!(t.text("networkInfo.ipv4AddressesPerInterface"), Some("10"));
        assert_eq!(
            t.text("networkInfo.networkPerformance"),
            Some("Up to 12.5 Gigabit")
        );
        assert_eq!(
            t.values("placementGroupInfo.supportedStrategies"),
            ["cluster", "partition", "spread"]
        );
    }

    #[test]
    fn old_xen_types_are_not_reported_as_nitro() {
        let t2 = spec("t2.micro");
        assert_eq!(t2.text("hypervisor"), Some("xen"));
        assert_eq!(t2.number("memoryInfo.sizeInMiB"), Some(1024.0));
        assert_eq!(
            t2.values("processorInfo.supportedArchitectures"),
            ["i386", "x86_64"]
        );
        assert!(t2.flag("burstablePerformanceSupported"));
        assert_eq!(
            t2.values("placementGroupInfo.supportedStrategies"),
            ["partition", "spread"]
        );

        let c1 = spec("c1.medium");
        assert!(!c1.flag("currentGeneration"));
        assert_eq!(c1.number("memoryInfo.sizeInMiB"), Some(1740.0));
        assert_eq!(
            c1.values("supportedVirtualizationTypes"),
            ["hvm", "paravirtual"]
        );
        assert_eq!(c1.text("networkInfo.ipv6Supported"), Some("false"));
        assert_eq!(c1.text("instanceStorageInfo.disks.sizeInGB"), Some("350"));
        assert_eq!(c1.text("instanceStorageInfo.disks.type"), Some("hdd"));
    }

    /// The carried-over records are what the catalog was built from before EC2's own
    /// data: EC2 describes these three no more, from any enabled Region.
    #[test]
    fn a_type_the_api_stopped_describing_is_still_known() {
        for name in ["c6gn.metal", "i2.large", "i3p.16xlarge"] {
            let t = spec(name);
            assert!(t.number("vCpuInfo.defaultVCpus").is_some(), "{name}");
            assert!(t.info.get("rebootMigrationSupport").is_none(), "{name}");
        }
    }

    #[test]
    fn a_type_is_offered_where_ec2_sells_it() {
        let only_gov = spec("i3p.16xlarge");
        assert!(only_gov.offered_in("us-gov-west-1"));
        assert!(!only_gov.offered_in("us-east-1"));
        // A Region the catalog does not know says nothing: whole catalog.
        assert!(only_gov.offered_in("cn-north-1"));
        assert!(catalog().iter().all(|t| t.offered_in("cn-north-1")));
        // The vCPU lookup quotas use does not depend on the Region.
        assert_eq!(default_vcpus("i3p.16xlarge"), Some(64));
        // Real offerings: a type dropped from a dumped Region is gone there.
        assert!(!spec("dl1.24xlarge").offered_in("us-east-1"));
        assert!(catalog()
            .iter()
            .filter(|t| t.offered_in("us-east-1"))
            .all(|t| t.values("rebootMigrationSupport").len() == 1));
        assert_eq!(
            catalog()
                .iter()
                .filter(|t| t.offered_in("us-east-1"))
                .count(),
            1369
        );
    }

    #[test]
    fn zones_come_from_the_offerings() {
        let m5 = spec("m5.large");
        assert_eq!(
            ["a", "b", "c", "d", "e", "f"]
                .iter()
                .filter(|l| m5.offered_in_zone("us-east-1", &format!("us-east-1{l}")))
                .count(),
            5
        );
        assert!(!m5.offered_in_zone("us-east-1", "us-east-1e"));
        let inf2 = spec("inf2.xlarge");
        assert!(!inf2.offered_in_zone("us-east-1", "us-east-1a"));
        assert!(inf2.offered_in_zone("us-east-1", "us-east-1b"));
        // Not sold in the Region at all.
        assert!(!spec("dl1.24xlarge").offered_in_zone("us-east-1", "us-east-1a"));
        // A letter the dumping account does not have (us-west-1 shows it a and c)
        // sells what the Region sells.
        assert!(m5.offered_in_zone("us-west-1", "us-west-1b"));
        // An opt-in Region's zones are not known: the Region's membership holds.
        let carried = catalog()
            .iter()
            .find(|t| t.offerings.get("af-south-1").is_some_and(String::is_empty))
            .expect("types carried over for af-south-1");
        assert!(carried.offered_in_zone("af-south-1", "af-south-1a"));
        assert!(carried.offered_in_zone("af-south-1", "af-south-1c"));
    }

    #[test]
    fn accelerators_come_from_the_api_blocks() {
        let device = |name: &str| {
            let mut found = spec(name).accelerators().unwrap();
            assert_eq!(found.len(), 1, "{name}");
            found.remove(0)
        };
        let accelerator = |kind, count, manufacturer, name| Accelerator {
            kind: Some(kind),
            count,
            manufacturer: Some(manufacturer),
            name: Some(name),
        };
        assert_eq!(
            device("g4dn.xlarge"),
            accelerator("gpu", 1.0, "nvidia", "t4")
        );
        // The real answer on dl1: GpuInfo, Habana, eight Gaudi HL-205.
        assert_eq!(
            device("dl1.24xlarge"),
            accelerator("gpu", 8.0, "habana", "gaudi-hl-205")
        );
        assert_eq!(
            spec("dl1.24xlarge").accelerator_memory_mib(),
            Some(262_144.0)
        );
        // A fractional GPU counts as the fraction of one it is.
        assert_eq!(
            device("g6f.2xlarge"),
            accelerator("gpu", 0.25, "nvidia", "l4")
        );
        assert_eq!(
            device("f1.2xlarge"),
            accelerator("fpga", 1.0, "xilinx", "vu9p")
        );
        assert_eq!(
            device("g4ad.xlarge"),
            accelerator("gpu", 1.0, "amd", "radeon-pro-v520")
        );
        assert_eq!(device("f2.12xlarge").kind, Some("fpga"));
        assert_eq!(device("f2.12xlarge").count, 2.0);
        assert_eq!(
            device("vt1.3xlarge"),
            accelerator("media", 1.0, "xilinx", "u30")
        );
        assert_eq!(spec("m5.large").accelerators(), Some(Vec::new()));
        assert_eq!(spec("m5.large").accelerator_memory_mib(), Some(0.0));
        // Carried over with basic specs: nothing is known about its devices.
        assert_eq!(spec("i3p.16xlarge").accelerators(), None);
        assert_eq!(spec("i3p.16xlarge").accelerator_memory_mib(), None);
    }

    /// Inferentia appears in both the inference and Neuron blocks of one response,
    /// and is not two chips for it. Trainium is only in the latter, and real EC2 does
    /// not list it under `AcceleratorTypes=[inference]`.
    #[test]
    fn neuron_devices_are_amazons() {
        assert_eq!(
            spec("trn1.2xlarge").accelerators().unwrap(),
            [Accelerator {
                kind: None,
                count: 1.0,
                manufacturer: Some("amazon-web-services"),
                name: Some("trainium"),
            }]
        );
        let inf1 = spec("inf1.xlarge");
        assert_eq!(
            inf1.accelerators().unwrap(),
            [Accelerator {
                kind: Some("inference"),
                count: 1.0,
                manufacturer: Some("amazon-web-services"),
                name: Some("inferentia"),
            }]
        );
        assert_eq!(inf1.accelerator_memory_mib(), Some(8192.0));
    }

    /// Every device name in the catalog either maps to a value of the enum real EC2
    /// recorded, or is deliberately listed as having none.
    #[test]
    fn every_accelerator_name_is_mapped_or_deliberately_not() {
        for (_, api) in ACCELERATOR_NAMES {
            assert!(ACCELERATOR_NAME_VALUES.contains(api), "{api}");
        }
        let mut seen = HashSet::new();
        for t in catalog() {
            for block in ACCELERATOR_BLOCKS {
                let path = format!("{}.{}.name", block.block, block.devices);
                seen.extend(t.values(&path));
            }
        }
        assert!(seen.len() > 20, "{seen:?}");
        for name in seen {
            let mapped = ACCELERATOR_NAMES.iter().any(|(n, _)| *n == name);
            assert!(
                mapped != UNNAMED_ACCELERATORS.contains(&name),
                "{name}: must be exactly one of mapped or deliberately unnamed"
            );
        }
    }

    /// Only these differ between Regions today. `matches` reads the merged record, so
    /// a new one here needs thought about requirements too.
    #[test]
    fn regions_differ_only_on_dedicated_hosts_and_usage_classes() {
        let mut overridden = HashSet::new();
        for t in catalog() {
            for elements in t.overrides.values() {
                overridden.extend(elements.keys().map(String::as_str));
            }
        }
        assert_eq!(
            overridden,
            HashSet::from(["dedicatedHostsSupported", "supportedUsageClasses"])
        );
    }

    /// As real EC2 answers `IncludeUnsupportedInRegion` per Region.
    #[test]
    fn the_unsupported_listing_is_per_region() {
        let c8gb = spec("c8gb.large");
        assert!(!c8gb.described_with_unsupported_in("ap-northeast-3"));
        assert!(c8gb.described_with_unsupported_in("us-east-1"));
        // Kept known wherever asked, though EC2 no longer describes it.
        assert!(spec("i3p.16xlarge").described_with_unsupported_in("us-east-1"));
        // A Region says what it says about a type it does not sell.
        let a1 = spec("a1.large");
        assert!(a1
            .values_in("us-east-1", "supportedUsageClasses")
            .contains(&"on-demand"));
    }

    /// Regions list a few types `DescribeInstanceTypes` does not describe, as recorded.
    #[test]
    fn requirements_listing_is_not_quite_what_a_region_sells() {
        let raw = raw_catalog();
        let omitted: HashSet<&str> = raw
            .requirements_omitted
            .values()
            .flatten()
            .map(String::as_str)
            .collect();
        assert_eq!(omitted, HashSet::from(["p5.4xlarge", "t1.micro"]));
        // Each Region only for what it sells.
        assert!(raw.requirements_omitted["us-east-2"].contains("p5.4xlarge"));
        assert!(!raw.requirements_omitted["us-east-2"].contains("t1.micro"));
        // A Region without its own data follows the others.
        assert!(!spec("t1.micro").listed_by_requirements_in("af-south-1"));
        let p5 = spec("p5.4xlarge");
        assert!(p5.offered_in("us-east-1"));
        assert!(!p5.listed_by_requirements_in("us-east-1"));
        let g6 = spec("g6.xlarge");
        assert!(!g6.offered_in("ap-southeast-1"));
        assert!(g6.listed_by_requirements_in("ap-southeast-1"));
    }

    #[test]
    fn a_region_reports_its_own_values() {
        let trn1 = spec("trn1.32xlarge");
        let classes = |region| trn1.values_in(region, "supportedUsageClasses");
        assert!(classes("us-east-1").contains(&"spot"));
        assert!(!classes("ap-south-1").contains(&"spot"));
        assert!(!trn1.info_xml("ap-south-1").contains("<item>spot</item>"));
        assert!(trn1.info_xml("us-east-1").contains("<item>spot</item>"));
        let c5 = spec("c5.12xlarge");
        let hosts = |region| c5.values_in(region, "dedicatedHostsSupported");
        assert_eq!(hosts("ap-northeast-1"), ["false"]);
        assert_eq!(hosts("us-west-2"), ["true"]);
    }

    #[test]
    fn the_inference_block_renders_its_member_entries() {
        let xml = spec("inf1.xlarge").info_xml("us-east-1");
        assert!(
            xml.contains("<inferenceAcceleratorInfo><accelerators><member>"),
            "{xml}"
        );
        assert!(!xml.contains("<accelerators><item>"), "{xml}");
    }

    #[test]
    fn bare_metal_has_no_hypervisor() {
        let t = spec("mac1.metal");
        assert!(t.flag("bareMetal"));
        assert_eq!(t.text("hypervisor"), None);
        assert_eq!(
            t.values("processorInfo.supportedArchitectures"),
            ["x86_64_mac"]
        );
        assert_eq!(
            spec("mac2.metal").values("processorInfo.supportedArchitectures"),
            ["arm64_mac"]
        );
    }

    #[test]
    fn capacity_block_types_are_the_ones_the_api_says() {
        let in_us_east_1 = capacity_block_types("us-east-1");
        for name in [
            "p4d.24xlarge",
            "p5.48xlarge",
            "p6-b300.48xlarge",
            "trn1.32xlarge",
        ] {
            assert!(in_us_east_1.contains(&name), "{name}");
        }
        assert!(!in_us_east_1.contains(&"m5.large"));
        // Scoped to the Region: EC2 sells this one in only two of them.
        assert!(!in_us_east_1.contains(&"trn2.48xlarge"));
        assert!(capacity_block_types("us-east-2").contains(&"trn2.48xlarge"));
    }

    #[test]
    fn filter_paths_reach_nested_lists() {
        let t = spec("i3.large");
        let v = |filter: &str| {
            let (_, path) = INSTANCE_TYPE_FILTERS
                .iter()
                .find(|(f, _)| *f == filter)
                .unwrap();
            t.values(path)
        };
        assert_eq!(v("memory-info.size-in-mib"), ["15616"]);
        assert_eq!(v("instance-storage-supported"), ["true"]);
        assert_eq!(v("instance-storage-info.disk.type"), ["ssd"]);
        assert_eq!(v("instance-storage-info.encryption-support"), ["required"]);
        assert_eq!(v("hypervisor"), ["xen"]);
        assert_eq!(v("current-generation"), ["true"]);
        assert_eq!(v("vcpu-info.valid-threads-per-core"), ["1", "2"]);
        assert!(spec("m5.large")
            .values("instanceStorageInfo.disks.type")
            .is_empty());
    }
}
