//! DescribeInstanceTypes serves the full catalog, honours filters, and returns
//! the EBS, network and placement-group blocks that SDK clients and the
//! Terraform AWS provider read.

mod helpers;

use aws_sdk_ec2::types::builders::InstanceRequirementsRequestBuilder;
use aws_sdk_ec2::types::{
    AcceleratorManufacturer, AcceleratorName, AcceleratorType, ArchitectureType, CpuManufacturer,
    EbsOptimizedSupport, Filter, InstanceGeneration, InstanceRequirementsRequest,
    InstanceTypeHypervisor, InstanceTypeInfo, LocalStorageType, LocationType, MemoryMiBRequest,
    PlacementGroupStrategy, TotalLocalStorageGbRequest, VCpuCountRangeRequest, VirtualizationType,
};
use helpers::TestServer;

async fn describe(
    c: &aws_sdk_ec2::Client,
    types: &[&str],
    filters: Vec<Filter>,
) -> Vec<aws_sdk_ec2::types::InstanceTypeInfo> {
    let mut req = c.describe_instance_types();
    for t in types {
        req = req.instance_types(t.parse().unwrap());
    }
    for f in filters {
        req = req.filters(f);
    }
    let mut out = Vec::new();
    let mut pages = req.into_paginator().send();
    while let Some(page) = pages.next().await {
        out.extend(page.unwrap().instance_types().to_vec());
    }
    out
}

fn filter(name: &str, value: &str) -> Filter {
    Filter::builder().name(name).values(value).build()
}

#[tokio::test]
async fn describes_graviton_and_x86_types_with_full_detail() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let items = describe(&c, &["m7g.large", "t3a.large", "t3.large"], vec![]).await;
    assert_eq!(items.len(), 3);
    for item in &items {
        let name = item.instance_type().unwrap().as_str();
        assert!(item.ebs_info().is_some(), "{name}: ebsInfo");
        assert!(item.network_info().is_some(), "{name}: networkInfo");
        assert!(
            item.placement_group_info().is_some(),
            "{name}: placementGroupInfo"
        );
    }

    let arch = |name: &str| {
        let item = items
            .iter()
            .find(|i| i.instance_type().unwrap().as_str() == name)
            .unwrap();
        item.processor_info().unwrap().supported_architectures()[0].clone()
    };
    assert_eq!(arch("m7g.large"), ArchitectureType::Arm64);
    assert_eq!(arch("t3a.large"), ArchitectureType::X8664);
    assert_eq!(arch("t3.large"), ArchitectureType::X8664);

    let m7g = items
        .iter()
        .find(|i| i.instance_type().unwrap().as_str() == "m7g.large")
        .unwrap();
    assert_eq!(m7g.v_cpu_info().unwrap().default_v_cpus(), Some(2));
}

#[tokio::test]
async fn instance_type_filter_supports_wildcards() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let items = describe(&c, &[], vec![filter("instance-type", "m7g.*")]).await;
    assert!(items.len() > 1);
    for item in &items {
        assert!(item.instance_type().unwrap().as_str().starts_with("m7g."));
    }
}

#[tokio::test]
async fn architecture_filter_separates_arm64_from_x86_64() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let arm = describe(
        &c,
        &[],
        vec![filter("processor-info.supported-architecture", "arm64")],
    )
    .await;
    assert!(!arm.is_empty());
    let names: Vec<_> = arm
        .iter()
        .map(|i| i.instance_type().unwrap().as_str().to_string())
        .collect();
    assert!(names.contains(&"m7g.large".to_string()));
    assert!(!names.contains(&"t3.large".to_string()));
    for item in &arm {
        assert_eq!(
            item.processor_info().unwrap().supported_architectures(),
            &[ArchitectureType::Arm64]
        );
    }
}

#[tokio::test]
async fn pages_with_max_results_and_visits_every_type_once() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let first = c
        .describe_instance_types()
        .max_results(5)
        .send()
        .await
        .unwrap();
    assert_eq!(first.instance_types().len(), 5);
    let token = first.next_token().expect("a next token").to_string();

    let second = c
        .describe_instance_types()
        .max_results(5)
        .next_token(token)
        .send()
        .await
        .unwrap();
    assert_eq!(second.instance_types().len(), 5);
    assert_ne!(
        first.instance_types()[0].instance_type(),
        second.instance_types()[0].instance_type()
    );

    let walked = describe(&c, &[], vec![]).await;
    let mut names: Vec<_> = walked
        .iter()
        .map(|i| i.instance_type().unwrap().as_str().to_string())
        .collect();
    let total = names.len();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), total, "a type was returned twice");
    assert!(total > 100);
}

#[tokio::test]
async fn unknown_instance_type_is_rejected() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let err = c
        .describe_instance_types()
        .instance_types("m7g.large".parse().unwrap())
        .instance_types("nope.large".parse().unwrap())
        .send()
        .await
        .expect_err("unknown type should fail");
    let text = format!("{err:?}");
    assert!(text.contains("InvalidInstanceType"), "{text}");
    assert!(text.contains("nope.large"), "{text}");
}

#[tokio::test]
async fn unknown_and_unsupported_filters_are_rejected() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let err = c
        .describe_instance_types()
        .filters(filter("no-such-filter", "x"))
        .send()
        .await
        .expect_err("unknown filter should fail");
    assert!(format!("{err:?}").contains("InvalidParameterValue"));

    let err = c
        .describe_instance_types()
        .filters(filter("network-info.efa-supported", "true"))
        .send()
        .await
        .expect_err("unsupported filter should fail");
    let text = format!("{err:?}");
    assert!(text.contains("network-info.efa-supported"), "{text}");
    assert!(text.contains("UnsupportedOperation"), "{text}");
}

#[tokio::test]
async fn mac_types_report_the_mac_architectures() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let items = describe(&c, &["mac1.metal", "mac2.metal"], vec![]).await;
    let arch = |name: &str| {
        items
            .iter()
            .find(|i| i.instance_type().unwrap().as_str() == name)
            .unwrap()
            .processor_info()
            .unwrap()
            .supported_architectures()[0]
            .clone()
    };
    assert_eq!(arch("mac1.metal"), ArchitectureType::X8664Mac);
    assert_eq!(arch("mac2.metal"), ArchitectureType::Arm64Mac);
}

async fn one(c: &aws_sdk_ec2::Client, name: &str) -> InstanceTypeInfo {
    describe(c, &[name], vec![]).await.remove(0)
}

#[tokio::test]
async fn reports_the_real_values_of_each_type() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let m7g = one(&c, "m7g.large").await;
    assert_eq!(m7g.memory_info().unwrap().size_in_mib(), Some(8192));
    assert_eq!(m7g.hypervisor(), Some(&InstanceTypeHypervisor::Nitro));
    assert_eq!(m7g.current_generation(), Some(true));
    assert_eq!(m7g.v_cpu_info().unwrap().default_cores(), Some(2));
    assert_eq!(
        m7g.v_cpu_info().unwrap().default_threads_per_core(),
        Some(1)
    );
    let ebs = m7g.ebs_info().unwrap();
    assert_eq!(
        ebs.ebs_optimized_support(),
        Some(&EbsOptimizedSupport::Default)
    );
    let optimized = ebs.ebs_optimized_info().unwrap();
    assert_eq!(optimized.baseline_throughput_in_m_bps(), Some(78.75));
    assert_eq!(optimized.maximum_iops(), Some(40000));
    let net = m7g.network_info().unwrap();
    assert_eq!(net.network_performance(), Some("Up to 12.5 Gigabit"));
    assert_eq!(net.maximum_network_interfaces(), Some(3));
    assert_eq!(net.ipv4_addresses_per_interface(), Some(10));

    let t2 = one(&c, "t2.micro").await;
    assert_eq!(t2.hypervisor(), Some(&InstanceTypeHypervisor::Xen));
    assert_eq!(t2.memory_info().unwrap().size_in_mib(), Some(1024));
    assert_eq!(t2.burstable_performance_supported(), Some(true));
    assert!(!t2
        .placement_group_info()
        .unwrap()
        .supported_strategies()
        .contains(&PlacementGroupStrategy::Cluster));

    let c1 = one(&c, "c1.medium").await;
    assert_eq!(c1.current_generation(), Some(false));
    assert_eq!(c1.memory_info().unwrap().size_in_mib(), Some(1740));
    assert_eq!(
        c1.supported_virtualization_types(),
        &[VirtualizationType::Paravirtual]
    );
    assert_eq!(c1.instance_storage_supported(), Some(true));
    let storage = c1.instance_storage_info().unwrap();
    assert_eq!(storage.total_size_in_gb(), Some(350));

    // No published values for this one: the blocks exist, the unknowns do not.
    let p3 = one(&c, "p3.8xlarge").await;
    assert_eq!(p3.hypervisor(), None);
    assert!(p3.ebs_info().is_some());
    assert!(p3.network_info().is_some());
    assert!(p3.placement_group_info().is_some());
    assert_eq!(p3.ebs_info().unwrap().ebs_optimized_support(), None);
}

#[tokio::test]
async fn data_backed_filters_narrow_the_catalog() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let items = describe(
        &c,
        &[],
        vec![
            filter("memory-info.size-in-mib", "8192"),
            filter("vcpu-info.default-vcpus", "2"),
            filter("hypervisor", "xen"),
        ],
    )
    .await;
    let names: Vec<_> = items
        .iter()
        .map(|i| i.instance_type().unwrap().as_str())
        .collect();
    assert!(names.contains(&"m4.large"), "{names:?}");
    assert!(!names.contains(&"m7g.large"), "{names:?}");
    assert!(!names.contains(&"m5.large"), "{names:?}");

    let previous = describe(&c, &[], vec![filter("current-generation", "false")]).await;
    let names: Vec<_> = previous
        .iter()
        .map(|i| i.instance_type().unwrap().as_str())
        .collect();
    assert!(names.contains(&"c1.medium"), "{names:?}");
    assert!(!names.contains(&"t2.micro"), "{names:?}");
}

#[tokio::test]
async fn get_instance_types_from_requirements_evaluates_the_catalog() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let requirements = aws_sdk_ec2::types::InstanceRequirementsRequest::builder()
        .v_cpu_count(VCpuCountRangeRequest::builder().min(2).max(2).build())
        .memory_mib(MemoryMiBRequest::builder().min(8192).max(8192).build())
        .build();
    let mut pages = c
        .get_instance_types_from_instance_requirements()
        .architecture_types(ArchitectureType::Arm64)
        .virtualization_types(VirtualizationType::Hvm)
        .instance_requirements(requirements)
        .into_paginator()
        .send();
    let mut names = Vec::new();
    while let Some(page) = pages.next().await {
        for t in page.unwrap().instance_types() {
            names.push(t.instance_type().unwrap().to_string());
        }
    }
    assert!(names.contains(&"m7g.large".to_string()), "{names:?}");
    assert!(!names.contains(&"m5.large".to_string()), "{names:?}");
    assert!(!names.contains(&"t3.large".to_string()), "{names:?}");
}

/// Every type the real SDK's serialization of `requirements` matches, on top of
/// the members EC2 requires.
async fn from_requirements(
    c: &aws_sdk_ec2::Client,
    requirements: InstanceRequirementsRequestBuilder,
) -> Vec<String> {
    let requirements: InstanceRequirementsRequest = requirements
        .v_cpu_count(VCpuCountRangeRequest::builder().min(1).build())
        .memory_mib(MemoryMiBRequest::builder().min(1).build())
        .build();
    let mut pages = c
        .get_instance_types_from_instance_requirements()
        .architecture_types(ArchitectureType::X8664)
        .architecture_types(ArchitectureType::Arm64)
        .virtualization_types(VirtualizationType::Hvm)
        .virtualization_types(VirtualizationType::Paravirtual)
        .instance_requirements(requirements)
        .into_paginator()
        .send();
    let mut names = Vec::new();
    while let Some(page) = pages.next().await {
        for t in page.unwrap().instance_types() {
            names.push(t.instance_type().unwrap().to_string());
        }
    }
    names
}

/// Each nested list reaches the handler under the name the SDK really sends, so
/// it narrows the result: `in_` must stay listed and `out` must not.
#[tokio::test]
async fn the_sdks_nested_requirement_lists_are_honored() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let builder = InstanceRequirementsRequest::builder;

    let cases: Vec<(&str, InstanceRequirementsRequestBuilder, &str, &str)> = vec![
        (
            "AcceleratorTypes",
            builder().accelerator_types(AcceleratorType::Gpu),
            "g4dn.xlarge",
            "inf1.xlarge",
        ),
        (
            "AcceleratorManufacturers",
            builder().accelerator_manufacturers(AcceleratorManufacturer::Nvidia),
            "g4dn.xlarge",
            "g4ad.xlarge",
        ),
        (
            "AcceleratorNames",
            builder().accelerator_names(AcceleratorName::T4),
            "g4dn.xlarge",
            "g5.xlarge",
        ),
        (
            "CpuManufacturers",
            builder().cpu_manufacturers(CpuManufacturer::AmazonWebServices),
            "m7g.large",
            "m5.large",
        ),
        (
            "InstanceGenerations",
            builder().instance_generations(InstanceGeneration::Previous),
            "c1.medium",
            "m5.large",
        ),
        (
            "AllowedInstanceTypes",
            builder().allowed_instance_types("m7g.*"),
            "m7g.large",
            "m5.large",
        ),
        (
            "ExcludedInstanceTypes",
            builder().excluded_instance_types("m5.*"),
            "m7g.large",
            "m5.large",
        ),
        (
            "LocalStorageTypes",
            builder().local_storage_types(LocalStorageType::Hdd),
            "d3.xlarge",
            "i3.large",
        ),
        (
            "TotalLocalStorageGB",
            builder()
                .total_local_storage_gb(TotalLocalStorageGbRequest::builder().min(1000.0).build()),
            "d3.xlarge",
            "m7g.large",
        ),
    ];
    let unconstrained = from_requirements(&c, builder()).await;
    for (member, requirements, in_, out) in cases {
        let names = from_requirements(&c, requirements).await;
        assert!(names.contains(&in_.to_string()), "{member}: {in_}");
        assert!(!names.contains(&out.to_string()), "{member}: {out}");
        assert!(names.len() < unconstrained.len(), "{member}");
    }
    // Without the list, the excluded examples are all listed.
    for name in [
        "inf1.xlarge",
        "g4ad.xlarge",
        "g5.xlarge",
        "m5.large",
        "i3.large",
    ] {
        assert!(unconstrained.contains(&name.to_string()), "{name}");
    }
}

#[tokio::test]
async fn instance_type_offerings_list_the_catalog_in_the_region() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let mut pages = c
        .describe_instance_type_offerings()
        .location_type(LocationType::Region)
        .max_results(100)
        .into_paginator()
        .send();
    let mut count = 0;
    let mut saw_m7g = false;
    while let Some(page) = pages.next().await {
        for o in page.unwrap().instance_type_offerings() {
            count += 1;
            saw_m7g |= o.instance_type().unwrap().as_str() == "m7g.large";
            assert_eq!(o.location_type(), Some(&LocationType::Region));
        }
    }
    assert!(count > 100, "{count}");
    assert!(saw_m7g);
}

#[tokio::test]
async fn the_catalog_follows_the_request_region() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let config = c
        .config()
        .to_builder()
        .region(aws_sdk_ec2::config::Region::new("eu-west-1"))
        .build();
    let eu = aws_sdk_ec2::Client::from_conf(config);

    assert!(one(&eu, "m5.large").await.ebs_info().is_some());
    let err = c
        .describe_instance_types()
        .instance_types("i3p.16xlarge".parse().unwrap())
        .send()
        .await
        .expect_err("not sold in us-east-1");
    assert!(
        format!("{err:?}").contains("InvalidInstanceType"),
        "{err:?}"
    );
    let included = c
        .describe_instance_types()
        .instance_types("i3p.16xlarge".parse().unwrap())
        .include_unsupported_in_region(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        included.instance_types()[0].supported_in_region(),
        Some(false)
    );
}
