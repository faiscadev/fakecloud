//! DescribeInstanceTypes serves the full catalog, honours filters, and returns
//! the EBS, network and placement-group blocks that SDK clients and the
//! Terraform AWS provider read.

mod helpers;

use aws_sdk_ec2::types::{ArchitectureType, Filter};
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
