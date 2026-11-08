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
        .filters(filter("memory-info.size-in-mib", "8192"))
        .send()
        .await
        .expect_err("unsupported filter should fail");
    let text = format!("{err:?}");
    assert!(text.contains("memory-info.size-in-mib"), "{text}");
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
