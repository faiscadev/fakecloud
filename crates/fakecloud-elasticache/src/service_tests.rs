use super::*;
use crate::state::default_engine_versions;
use bytes::Bytes;
use http::{HeaderMap, Method};
use std::collections::HashMap;

fn request(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut query_params = HashMap::from([("Action".to_string(), action.to_string())]);
    for (key, value) in params {
        query_params.insert((*key).to_string(), (*value).to_string());
    }

    AwsRequest {
        service: "elasticache".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request-id".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

fn sample_reserved_cache_node_offering(id: &str) -> ReservedCacheNodesOffering {
    ReservedCacheNodesOffering {
        reserved_cache_nodes_offering_id: id.to_string(),
        cache_node_type: "cache.t3.micro".to_string(),
        duration: 31_536_000,
        fixed_price: 0.0,
        usage_price: 0.011,
        product_description: "redis".to_string(),
        offering_type: "No Upfront".to_string(),
        recurring_charges: Vec::new(),
    }
}

fn sample_reserved_cache_node(id: &str, offering_id: &str) -> ReservedCacheNode {
    ReservedCacheNode {
        reserved_cache_node_id: id.to_string(),
        reserved_cache_nodes_offering_id: offering_id.to_string(),
        cache_node_type: "cache.t3.micro".to_string(),
        start_time: "2024-01-01T00:00:00Z".to_string(),
        duration: 31_536_000,
        fixed_price: 0.0,
        usage_price: 0.011,
        cache_node_count: 1,
        product_description: "redis".to_string(),
        offering_type: "No Upfront".to_string(),
        state: "payment-pending".to_string(),
        recurring_charges: Vec::new(),
        reservation_arn: "arn:aws:elasticache:us-east-1:123456789012:reserved-instance:test"
            .to_string(),
    }
}

#[test]
fn parse_member_list_extracts_indexed_values() {
    let mut params = HashMap::new();
    params.insert(
        "SubnetIds.SubnetIdentifier.1".to_string(),
        "subnet-aaa".to_string(),
    );
    params.insert(
        "SubnetIds.SubnetIdentifier.2".to_string(),
        "subnet-bbb".to_string(),
    );
    params.insert(
        "SubnetIds.SubnetIdentifier.3".to_string(),
        "subnet-ccc".to_string(),
    );
    params.insert("OtherParam".to_string(), "ignored".to_string());

    let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
    assert_eq!(result, vec!["subnet-aaa", "subnet-bbb", "subnet-ccc"]);
}

#[test]
fn parse_member_list_returns_sorted_by_index() {
    let mut params = HashMap::new();
    params.insert(
        "SubnetIds.SubnetIdentifier.3".to_string(),
        "subnet-ccc".to_string(),
    );
    params.insert(
        "SubnetIds.SubnetIdentifier.1".to_string(),
        "subnet-aaa".to_string(),
    );

    let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
    assert_eq!(result, vec!["subnet-aaa", "subnet-ccc"]);
}

#[test]
fn parse_member_list_returns_empty_for_no_matches() {
    let params = HashMap::new();
    let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
    assert!(result.is_empty());
}

#[test]
fn cache_subnet_group_xml_contains_all_fields() {
    let group = CacheSubnetGroup {
        cache_subnet_group_name: "my-group".to_string(),
        cache_subnet_group_description: "My description".to_string(),
        vpc_id: "vpc-123".to_string(),
        subnet_ids: vec!["subnet-aaa".to_string(), "subnet-bbb".to_string()],
        arn: "arn:aws:elasticache:us-east-1:123:subnetgroup:my-group".to_string(),
    };
    let xml = cache_subnet_group_xml(&group, "us-east-1");
    assert!(xml.contains("<CacheSubnetGroupName>my-group</CacheSubnetGroupName>"));
    assert!(
        xml.contains("<CacheSubnetGroupDescription>My description</CacheSubnetGroupDescription>")
    );
    assert!(xml.contains("<VpcId>vpc-123</VpcId>"));
    assert!(xml.contains("<SubnetIdentifier>subnet-aaa</SubnetIdentifier>"));
    assert!(xml.contains("<SubnetIdentifier>subnet-bbb</SubnetIdentifier>"));
    assert!(xml.contains("<Name>us-east-1a</Name>"));
    assert!(xml.contains("<Name>us-east-1b</Name>"));
    assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:subnetgroup:my-group</ARN>"));
}

#[test]
fn cache_cluster_xml_contains_expected_fields() {
    let cluster = CacheCluster {
        cache_cluster_id: "classic-1".to_string(),
        cache_node_type: "cache.t3.micro".to_string(),
        engine: "redis".to_string(),
        engine_version: "7.1".to_string(),
        cache_cluster_status: "available".to_string(),
        num_cache_nodes: 2,
        preferred_availability_zone: "us-east-1a".to_string(),
        cache_subnet_group_name: Some("default".to_string()),
        auto_minor_version_upgrade: true,
        arn: "arn:aws:elasticache:us-east-1:123:cluster:classic-1".to_string(),
        created_at: "2024-01-01T00:00:00Z".to_string(),
        endpoint_address: "127.0.0.1".to_string(),
        endpoint_port: 6379,
        container_id: "abc123".to_string(),
        host_port: 6379,
        replication_group_id: Some("rg-1".to_string()),
        cache_parameter_group_name: None,
        security_group_ids: Vec::new(),
        log_delivery_configurations: Vec::new(),
        transit_encryption_enabled: false,
        at_rest_encryption_enabled: false,
        auth_token_enabled: false,
        port: 6379,
        preferred_maintenance_window: None,
        preferred_availability_zones: Vec::new(),
        notification_topic_arn: None,
        cache_security_group_names: Vec::new(),
        snapshot_arns: Vec::new(),
        snapshot_name: None,
        snapshot_retention_limit: 0,
        snapshot_window: None,
        outpost_mode: None,
        preferred_outpost_arn: None,
        network_type: None,
        ip_discovery: None,
        az_mode: None,
        auth_token: None,
        kms_key_id: None,
        transit_encryption_mode: None,
        data_tiering_enabled: None,
        cluster_mode: None,
        preferred_outpost_arns: Vec::new(),
    };
    let xml = cache_cluster_xml(&cluster, true);
    assert!(xml.contains("<CacheClusterId>classic-1</CacheClusterId>"));
    assert!(xml.contains("<CacheNodeType>cache.t3.micro</CacheNodeType>"));
    assert!(xml.contains("<Engine>redis</Engine>"));
    assert!(xml.contains("<NumCacheNodes>2</NumCacheNodes>"));
    assert!(xml.contains("<PreferredAvailabilityZone>us-east-1a</PreferredAvailabilityZone>"));
    assert!(xml.contains("<CacheSubnetGroupName>default</CacheSubnetGroupName>"));
    assert!(xml.contains("<CacheNodes>"));
    assert!(xml.contains("<CacheNodeId>0001</CacheNodeId>"));
    assert!(xml.contains("<ReplicationGroupId>rg-1</ReplicationGroupId>"));
    assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:cluster:classic-1</ARN>"));
}

#[test]
fn filter_engine_versions_by_engine() {
    let versions = default_engine_versions();
    let filtered = filter_engine_versions(&versions, &Some("redis".to_string()), &None, &None);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].engine, "redis");
}

#[test]
fn filter_engine_versions_by_family() {
    let versions = default_engine_versions();
    let filtered = filter_engine_versions(&versions, &None, &None, &Some("valkey8".to_string()));
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].engine, "valkey");
}

#[test]
fn filter_engine_versions_by_memcached() {
    let versions = default_engine_versions();
    let filtered = filter_engine_versions(&versions, &Some("memcached".to_string()), &None, &None);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].engine, "memcached");
}

#[test]
fn filter_engine_versions_unknown_engine() {
    let versions = default_engine_versions();
    let filtered = filter_engine_versions(&versions, &Some("oracle".to_string()), &None, &None);
    assert!(filtered.is_empty());
}

#[test]
fn paginate_returns_all_when_within_limit() {
    let items = vec![1, 2, 3];
    let (page, marker) = paginate(&items, None, None).unwrap();
    assert_eq!(page, vec![1, 2, 3]);
    assert!(marker.is_none());
}

#[test]
fn paginate_respects_max_records() {
    let items = vec![1, 2, 3, 4, 5];
    let (page, marker) = paginate(&items, None, Some(2)).unwrap();
    assert_eq!(page, vec![1, 2]);
    assert_eq!(marker, Some("2".to_string()));

    let (page2, marker2) = paginate(&items, Some("2"), Some(2)).unwrap();
    assert_eq!(page2, vec![3, 4]);
    assert_eq!(marker2, Some("4".to_string()));

    let (page3, marker3) = paginate(&items, Some("4"), Some(2)).unwrap();
    assert_eq!(page3, vec![5]);
    assert!(marker3.is_none());
}

#[test]
fn paginate_rejects_invalid_marker() {
    // bug-audit 2026-05-28, 1.7: a Marker that does not parse as a valid offset
    // must surface InvalidParameterValue rather than being silently treated as
    // the first page (which can drive an infinite client pagination loop).
    let items = vec![1, 2, 3, 4, 5];
    let err =
        paginate(&items, Some("not-a-number"), Some(2)).expect_err("malformed Marker must error");
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[test]
fn parse_reserved_duration_filter_accepts_years_and_seconds() {
    assert_eq!(
        parse_reserved_duration_filter(Some("1".to_string())).unwrap(),
        Some(31_536_000)
    );
    assert_eq!(
        parse_reserved_duration_filter(Some("94608000".to_string())).unwrap(),
        Some(94_608_000)
    );
}

#[test]
fn parse_reserved_duration_filter_rejects_invalid_value() {
    assert!(parse_reserved_duration_filter(Some("2".to_string())).is_err());
}

#[test]
fn xml_wrap_produces_valid_response() {
    let xml = query_response_xml("TestAction", ELASTICACHE_NS, "<Data>ok</Data>", "req-123");
    assert!(xml.contains("<TestActionResponse"));
    assert!(xml.contains("<TestActionResult>"));
    assert!(xml.contains("<RequestId>req-123</RequestId>"));
    assert!(xml.contains(ELASTICACHE_NS));
}

#[test]
fn parse_tags_reads_query_shape() {
    let req = request(
        "AddTagsToResource",
        &[
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "prod"),
            ("Tags.Tag.2.Key", "team"),
            ("Tags.Tag.2.Value", "backend"),
        ],
    );

    let tags = parse_tags(&req).expect("tags");
    assert_eq!(
        tags,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "backend".to_string()),
        ]
    );
}

#[test]
fn parse_tags_returns_empty_for_no_tags() {
    let req = request("AddTagsToResource", &[]);
    let tags = parse_tags(&req).expect("tags");
    assert!(tags.is_empty());
}

#[test]
fn parse_tag_keys_reads_member_shape() {
    let req = request(
        "RemoveTagsFromResource",
        &[("TagKeys.member.1", "env"), ("TagKeys.member.2", "team")],
    );

    let keys = parse_tag_keys(&req).expect("tag keys");
    assert_eq!(keys, vec!["env".to_string(), "team".to_string()]);
}

#[test]
fn merge_tags_adds_new_and_updates_existing() {
    let mut tags = vec![("env".to_string(), "dev".to_string())];

    merge_tags(
        &mut tags,
        &[
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "core".to_string()),
        ],
    );

    assert_eq!(
        tags,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "core".to_string()),
        ]
    );
}

#[test]
fn tag_xml_produces_valid_element() {
    let xml = tag_xml(&("env".to_string(), "prod".to_string()));
    assert_eq!(xml, "<Tag><Key>env</Key><Value>prod</Value></Tag>");
}

#[test]
fn reserved_cache_nodes_offering_xml_contains_expected_fields() {
    let xml = reserved_cache_nodes_offering_xml(&ReservedCacheNodesOffering {
        reserved_cache_nodes_offering_id: "offering-a".to_string(),
        cache_node_type: "cache.r6g.large".to_string(),
        duration: 94_608_000,
        fixed_price: 1550.0,
        usage_price: 0.0,
        product_description: "redis".to_string(),
        offering_type: "All Upfront".to_string(),
        recurring_charges: vec![RecurringCharge {
            recurring_charge_amount: 0.0,
            recurring_charge_frequency: "Hourly".to_string(),
        }],
    });
    assert!(xml.contains("<ReservedCacheNodesOfferingId>offering-a</ReservedCacheNodesOfferingId>"));
    assert!(xml.contains("<CacheNodeType>cache.r6g.large</CacheNodeType>"));
    assert!(xml.contains("<Duration>94608000</Duration>"));
    assert!(xml.contains("<OfferingType>All Upfront</OfferingType>"));
    assert!(xml.contains("<RecurringChargeFrequency>Hourly</RecurringChargeFrequency>"));
}

#[test]
fn reserved_cache_node_xml_contains_expected_fields() {
    let xml = reserved_cache_node_xml(&sample_reserved_cache_node("rcn-a", "offering-a"));
    assert!(xml.contains("<ReservedCacheNodeId>rcn-a</ReservedCacheNodeId>"));
    assert!(xml.contains("<ReservedCacheNodesOfferingId>offering-a</ReservedCacheNodesOfferingId>"));
    assert!(xml.contains("<StartTime>2024-01-01T00:00:00Z</StartTime>"));
    assert!(xml.contains("<State>payment-pending</State>"));
    assert!(xml.contains("<ReservationARN>"));
}

#[test]
fn user_xml_contains_all_fields() {
    let user = ElastiCacheUser {
        user_id: "myuser".to_string(),
        user_name: "myuser".to_string(),
        engine: "redis".to_string(),
        access_string: "on ~* +@all".to_string(),
        status: "active".to_string(),
        authentication_type: "password".to_string(),
        password_count: 1,
        arn: "arn:aws:elasticache:us-east-1:123:user:myuser".to_string(),
        minimum_engine_version: "6.0".to_string(),
        user_group_ids: vec!["group1".to_string()],
    };
    let xml = user_xml(&user);
    assert!(xml.contains("<UserId>myuser</UserId>"));
    assert!(xml.contains("<UserName>myuser</UserName>"));
    assert!(xml.contains("<Engine>redis</Engine>"));
    assert!(xml.contains("<AccessString>on ~* +@all</AccessString>"));
    assert!(xml.contains("<Status>active</Status>"));
    assert!(xml.contains("<Type>password</Type>"));
    assert!(xml.contains("<PasswordCount>1</PasswordCount>"));
    assert!(xml.contains("<member>group1</member>"));
    assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:user:myuser</ARN>"));
}

#[test]
fn user_group_xml_contains_all_fields() {
    let group = ElastiCacheUserGroup {
        user_group_id: "mygroup".to_string(),
        engine: "redis".to_string(),
        status: "active".to_string(),
        user_ids: vec!["default".to_string(), "myuser".to_string()],
        arn: "arn:aws:elasticache:us-east-1:123:usergroup:mygroup".to_string(),
        minimum_engine_version: "6.0".to_string(),
        pending_changes: None,
        replication_groups: Vec::new(),
    };
    let xml = user_group_xml(&group);
    assert!(xml.contains("<UserGroupId>mygroup</UserGroupId>"));
    assert!(xml.contains("<Engine>redis</Engine>"));
    assert!(xml.contains("<Status>active</Status>"));
    assert!(xml.contains("<member>default</member>"));
    assert!(xml.contains("<member>myuser</member>"));
    assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:usergroup:mygroup</ARN>"));
}

#[test]
fn create_user_returns_user_xml() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateUser",
        &[
            ("UserId", "testuser"),
            ("UserName", "testuser"),
            ("Engine", "redis"),
            ("AccessString", "on ~* +@all"),
        ],
    );
    let resp = service.create_user(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<UserId>testuser</UserId>"));
    assert!(body.contains("<Status>active</Status>"));
    assert!(body.contains("<CreateUserResponse"));
}

#[test]
fn create_user_rejects_duplicate() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateUser",
        &[
            ("UserId", "default"),
            ("UserName", "default"),
            ("Engine", "redis"),
            ("AccessString", "on ~* +@all"),
        ],
    );
    assert!(service.create_user(&req).is_err());
}

#[test]
fn delete_user_rejects_default() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request("DeleteUser", &[("UserId", "default")]);
    assert!(service.delete_user(&req).is_err());
}

#[test]
fn describe_users_returns_default_user() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request("DescribeUsers", &[]);
    let resp = service.describe_users(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<UserId>default</UserId>"));
}

#[test]
fn describe_reserved_cache_nodes_returns_empty_list_by_default() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let resp = service
        .describe_reserved_cache_nodes(&request("DescribeReservedCacheNodes", &[]))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ReservedCacheNodes></ReservedCacheNodes>"));
}

#[test]
fn describe_reserved_cache_nodes_filters_by_offering_id() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        state.reserved_cache_nodes.insert(
            "rcn-a".to_string(),
            sample_reserved_cache_node("rcn-a", "offering-a"),
        );
        state.reserved_cache_nodes.insert(
            "rcn-b".to_string(),
            sample_reserved_cache_node("rcn-b", "offering-b"),
        );
    }

    let resp = service
        .describe_reserved_cache_nodes(&request(
            "DescribeReservedCacheNodes",
            &[("ReservedCacheNodesOfferingId", "offering-b")],
        ))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ReservedCacheNodeId>rcn-b</ReservedCacheNodeId>"));
    assert!(!body.contains("<ReservedCacheNodeId>rcn-a</ReservedCacheNodeId>"));
}

#[test]
fn describe_reserved_cache_nodes_not_found_by_id() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    assert!(service
        .describe_reserved_cache_nodes(&request(
            "DescribeReservedCacheNodes",
            &[("ReservedCacheNodeId", "missing")],
        ))
        .is_err());
}

#[test]
fn describe_reserved_cache_nodes_offerings_filters_and_paginates() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        state.reserved_cache_nodes_offerings = vec![
            sample_reserved_cache_node_offering("offering-a"),
            ReservedCacheNodesOffering {
                reserved_cache_nodes_offering_id: "offering-b".to_string(),
                cache_node_type: "cache.m5.large".to_string(),
                duration: 94_608_000,
                fixed_price: 0.0,
                usage_price: 0.033,
                product_description: "memcached".to_string(),
                offering_type: "No Upfront".to_string(),
                recurring_charges: Vec::new(),
            },
            ReservedCacheNodesOffering {
                reserved_cache_nodes_offering_id: "offering-c".to_string(),
                cache_node_type: "cache.r6g.large".to_string(),
                duration: 94_608_000,
                fixed_price: 1_550.0,
                usage_price: 0.0,
                product_description: "redis".to_string(),
                offering_type: "All Upfront".to_string(),
                recurring_charges: vec![RecurringCharge {
                    recurring_charge_amount: 0.0,
                    recurring_charge_frequency: "Hourly".to_string(),
                }],
            },
        ];
    }

    let filtered = service
        .describe_reserved_cache_nodes_offerings(&request(
            "DescribeReservedCacheNodesOfferings",
            &[("ProductDescription", "redis"), ("Duration", "3")],
        ))
        .unwrap();
    let filtered_body = String::from_utf8(filtered.body.expect_bytes().to_vec()).unwrap();
    assert!(filtered_body
        .contains("<ReservedCacheNodesOfferingId>offering-c</ReservedCacheNodesOfferingId>"));
    assert!(!filtered_body
        .contains("<ReservedCacheNodesOfferingId>offering-b</ReservedCacheNodesOfferingId>"));

    let paged = service
        .describe_reserved_cache_nodes_offerings(&request(
            "DescribeReservedCacheNodesOfferings",
            &[("MaxRecords", "1")],
        ))
        .unwrap();
    let paged_body = String::from_utf8(paged.body.expect_bytes().to_vec()).unwrap();
    assert!(paged_body.contains("<Marker>1</Marker>"));
}

#[test]
fn describe_reserved_cache_nodes_offerings_not_found_by_id() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    assert!(service
        .describe_reserved_cache_nodes_offerings(&request(
            "DescribeReservedCacheNodesOfferings",
            &[("ReservedCacheNodesOfferingId", "missing")],
        ))
        .is_err());
}

#[test]
fn create_and_describe_user_group() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateUserGroup",
        &[
            ("UserGroupId", "mygroup"),
            ("Engine", "redis"),
            ("UserIds.member.1", "default"),
        ],
    );
    let resp = service.create_user_group(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<UserGroupId>mygroup</UserGroupId>"));
    assert!(body.contains("<member>default</member>"));

    let req = request("DescribeUserGroups", &[]);
    let resp = service.describe_user_groups(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<UserGroupId>mygroup</UserGroupId>"));
}

#[test]
fn create_user_group_rejects_unknown_user() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateUserGroup",
        &[
            ("UserGroupId", "mygroup"),
            ("Engine", "redis"),
            ("UserIds.member.1", "nonexistent"),
        ],
    );
    assert!(service.create_user_group(&req).is_err());
}

#[test]
fn delete_user_group_removes_from_state() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateUserGroup",
        &[("UserGroupId", "delgroup"), ("Engine", "redis")],
    );
    service.create_user_group(&req).unwrap();

    let req = request("DeleteUserGroup", &[("UserGroupId", "delgroup")]);
    let resp = service.delete_user_group(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Status>deleting</Status>"));

    let req = request("DescribeUserGroups", &[("UserGroupId", "delgroup")]);
    assert!(service.describe_user_groups(&req).is_err());
}

fn service_with_cache_cluster(cluster_id: &str) -> ElastiCacheService {
    let shared: SharedElastiCacheState = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut __a = shared.write();
        let s = __a.default_mut();
        let arn = format!("arn:aws:elasticache:us-east-1:123456789012:cluster:{cluster_id}");
        s.tags.insert(arn.clone(), Vec::new());
        s.cache_clusters.insert(
            cluster_id.to_string(),
            CacheCluster {
                cache_cluster_id: cluster_id.to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                cache_cluster_status: "available".to_string(),
                num_cache_nodes: 1,
                preferred_availability_zone: "us-east-1a".to_string(),
                cache_subnet_group_name: Some("default".to_string()),
                auto_minor_version_upgrade: true,
                arn,
                created_at: "2024-01-01T00:00:00Z".to_string(),
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6379,
                container_id: "abc123".to_string(),
                host_port: 6379,
                replication_group_id: None,
                cache_parameter_group_name: None,
                security_group_ids: Vec::new(),
                log_delivery_configurations: Vec::new(),
                transit_encryption_enabled: false,
                at_rest_encryption_enabled: false,
                auth_token_enabled: false,
                port: 6379,
                preferred_maintenance_window: None,
                preferred_availability_zones: Vec::new(),
                notification_topic_arn: None,
                cache_security_group_names: Vec::new(),
                snapshot_arns: Vec::new(),
                snapshot_name: None,
                snapshot_retention_limit: 0,
                snapshot_window: None,
                outpost_mode: None,
                preferred_outpost_arn: None,
                network_type: None,
                ip_discovery: None,
                az_mode: None,
                auth_token: None,
                kms_key_id: None,
                transit_encryption_mode: None,
                data_tiering_enabled: None,
                cluster_mode: None,
                preferred_outpost_arns: Vec::new(),
            },
        );
    }
    ElastiCacheService::new(shared)
}

#[test]
fn describe_cache_clusters_returns_all() {
    let service = service_with_cache_cluster("cluster-a");
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        let arn = "arn:aws:elasticache:us-east-1:123456789012:cluster:cluster-b".to_string();
        state.tags.insert(arn.clone(), Vec::new());
        state.cache_clusters.insert(
            "cluster-b".to_string(),
            CacheCluster {
                cache_cluster_id: "cluster-b".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "valkey".to_string(),
                engine_version: "8.0".to_string(),
                cache_cluster_status: "available".to_string(),
                num_cache_nodes: 2,
                preferred_availability_zone: "us-east-1b".to_string(),
                cache_subnet_group_name: Some("default".to_string()),
                auto_minor_version_upgrade: false,
                arn,
                created_at: "2024-01-02T00:00:00Z".to_string(),
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6380,
                container_id: "def456".to_string(),
                host_port: 6380,
                replication_group_id: None,
                cache_parameter_group_name: None,
                security_group_ids: Vec::new(),
                log_delivery_configurations: Vec::new(),
                transit_encryption_enabled: false,
                at_rest_encryption_enabled: false,
                auth_token_enabled: false,
                port: 6380,
                preferred_maintenance_window: None,
                preferred_availability_zones: Vec::new(),
                notification_topic_arn: None,
                cache_security_group_names: Vec::new(),
                snapshot_arns: Vec::new(),
                snapshot_name: None,
                snapshot_retention_limit: 0,
                snapshot_window: None,
                outpost_mode: None,
                preferred_outpost_arn: None,
                network_type: None,
                ip_discovery: None,
                az_mode: None,
                auth_token: None,
                kms_key_id: None,
                transit_encryption_mode: None,
                data_tiering_enabled: None,
                cluster_mode: None,
                preferred_outpost_arns: Vec::new(),
            },
        );
    }

    let req = request("DescribeCacheClusters", &[]);
    let resp = service.describe_cache_clusters(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheClusterId>cluster-a</CacheClusterId>"));
    assert!(body.contains("<CacheClusterId>cluster-b</CacheClusterId>"));
    assert!(body.contains("<DescribeCacheClustersResponse"));
}

#[tokio::test]
async fn create_cache_cluster_validates_engine_before_runtime() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateCacheCluster",
        &[("CacheClusterId", "bad-engine"), ("Engine", "oracle")],
    );
    assert!(service.create_cache_cluster(&req).await.is_err());
}

#[tokio::test]
async fn create_replication_group_rejects_memcached() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-mc"),
            ("ReplicationGroupDescription", "no memcached"),
            ("Engine", "memcached"),
        ],
    );
    assert!(service.create_replication_group(&req).await.is_err());
}

#[tokio::test]
async fn create_serverless_cache_rejects_memcached() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let req = request(
        "CreateServerlessCache",
        &[("ServerlessCacheName", "sc-mc"), ("Engine", "memcached")],
    );
    assert!(service.create_serverless_cache(&req).await.is_err());
}

#[tokio::test]
async fn create_cache_cluster_without_runtime_falls_back_to_metadata_only() {
    // No container runtime is configured. The Smithy model has no
    // "Docker missing" error shape, so refusing the call would emit an
    // undeclared wire code. Instead the handler creates a metadata-only
    // cluster (no backing container, host_port=0) and returns success.
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared.clone());

    let req = request("CreateCacheCluster", &[("CacheClusterId", "no-runtime")]);
    let resp = service
        .create_cache_cluster(&req)
        .await
        .expect("metadata-only create");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheClusterId>no-runtime</CacheClusterId>"));

    let __a = shared.read();
    let state = __a.get("123456789012").expect("account state");
    let cluster = state.cache_clusters.get("no-runtime").expect("persisted");
    assert!(cluster.container_id.is_empty());
    assert_eq!(cluster.host_port, 0);
}

#[test]
fn describe_cache_clusters_filters_by_id_and_shows_node_info() {
    let service = service_with_cache_cluster("nodeful-cluster");
    let req = request(
        "DescribeCacheClusters",
        &[
            ("CacheClusterId", "nodeful-cluster"),
            ("ShowCacheNodeInfo", "true"),
        ],
    );
    let resp = service.describe_cache_clusters(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheClusterId>nodeful-cluster</CacheClusterId>"));
    assert!(body.contains("<CacheNodes>"));
    assert!(body.contains("<CacheNodeId>0001</CacheNodeId>"));
    assert!(body.contains("<ParameterGroupStatus>in-sync</ParameterGroupStatus>"));
}

#[test]
fn describe_cache_clusters_not_found() {
    let service = service_with_cache_cluster("cluster-a");
    let req = request("DescribeCacheClusters", &[("CacheClusterId", "missing")]);
    assert!(service.describe_cache_clusters(&req).is_err());
}

#[tokio::test]
async fn delete_cache_cluster_removes_state_and_tags() {
    let service = service_with_cache_cluster("delete-me");
    let arn = "arn:aws:elasticache:us-east-1:123456789012:cluster:delete-me".to_string();

    let req = request("DeleteCacheCluster", &[("CacheClusterId", "delete-me")]);
    let resp = service.delete_cache_cluster(&req).await.unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheClusterStatus>deleting</CacheClusterStatus>"));
    assert!(body.contains("<DeleteCacheClusterResponse"));
    assert!(!service
        .state
        .read()
        .default_ref()
        .cache_clusters
        .contains_key("delete-me"));
    assert!(!service.state.read().default_ref().tags.contains_key(&arn));
}

#[test]
fn add_cluster_to_replication_group_updates_members_and_count() {
    let mut state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
    state.replication_groups.insert(
        "rg-1".to_string(),
        ReplicationGroup {
            replication_group_id: "rg-1".to_string(),
            description: "test group".to_string(),
            global_replication_group_id: None,
            global_replication_group_role: None,
            status: "available".to_string(),
            cache_node_type: "cache.t3.micro".to_string(),
            engine: "redis".to_string(),
            engine_version: "7.1".to_string(),
            num_cache_clusters: 1,
            automatic_failover_enabled: false,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: 6379,
            arn: "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:rg-1".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            container_id: "abc123".to_string(),
            host_port: 6379,
            member_clusters: vec!["rg-1-001".to_string()],
            snapshot_retention_limit: 0,
            snapshot_window: "05:00-09:00".to_string(),
            transit_encryption_enabled: false,
            at_rest_encryption_enabled: false,
            cluster_enabled: false,
            kms_key_id: None,
            auth_token_enabled: false,
            user_group_ids: Vec::new(),
            multi_az_enabled: false,
            log_delivery_configurations: Vec::new(),
            data_tiering: None,
            ip_discovery: None,
            network_type: None,
            transit_encryption_mode: None,
            num_node_groups: 1,
            configuration_endpoint_address: None,
            configuration_endpoint_port: None,
            replicas_per_node_group: None,
            auth_token: None,
            port: 6379,
            notification_topic_arn: None,
            cluster_mode: None,
            data_tiering_enabled: None,
            notification_topic_status: None,
            cache_parameter_group_name: None,
            cache_subnet_group_name: None,
            security_group_ids: Vec::new(),
            preferred_maintenance_window: None,
            snapshot_name: None,
            snapshot_arns: Vec::new(),
            auto_minor_version_upgrade: true,
        },
    );

    add_cluster_to_replication_group(&mut state, "rg-1", "manual-cluster");

    let group = state.replication_groups.get("rg-1").unwrap();
    assert_eq!(group.member_clusters, vec!["rg-1-001", "manual-cluster"]);
    assert_eq!(group.num_cache_clusters, 2);
}

#[tokio::test]
async fn delete_cache_cluster_removes_cluster_from_replication_group() {
    let service = service_with_cache_cluster("delete-rg-cluster");
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        state
            .cache_clusters
            .get_mut("delete-rg-cluster")
            .unwrap()
            .replication_group_id = Some("delete-rg".to_string());
        state.replication_groups.insert(
            "delete-rg".to_string(),
            ReplicationGroup {
                replication_group_id: "delete-rg".to_string(),
                description: "test group".to_string(),
                global_replication_group_id: None,
                global_replication_group_role: None,
                status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                num_cache_clusters: 2,
                automatic_failover_enabled: false,
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6379,
                arn: "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:delete-rg"
                    .to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                container_id: "abc123".to_string(),
                host_port: 6379,
                member_clusters: vec!["delete-rg-001".to_string(), "delete-rg-cluster".to_string()],
                snapshot_retention_limit: 0,
                snapshot_window: "05:00-09:00".to_string(),
                transit_encryption_enabled: false,
                at_rest_encryption_enabled: false,
                cluster_enabled: false,
                kms_key_id: None,
                auth_token_enabled: false,
                user_group_ids: Vec::new(),
                multi_az_enabled: false,
                log_delivery_configurations: Vec::new(),
                data_tiering: None,
                ip_discovery: None,
                network_type: None,
                transit_encryption_mode: None,
                num_node_groups: 1,
                configuration_endpoint_address: None,
                configuration_endpoint_port: None,
                replicas_per_node_group: None,
                auth_token: None,
                port: 6379,
                notification_topic_arn: None,
                cluster_mode: None,
                data_tiering_enabled: None,
                notification_topic_status: None,
                cache_parameter_group_name: None,
                cache_subnet_group_name: None,
                security_group_ids: Vec::new(),
                preferred_maintenance_window: None,
                snapshot_name: None,
                snapshot_arns: Vec::new(),
                auto_minor_version_upgrade: true,
            },
        );
    }

    let req = request(
        "DeleteCacheCluster",
        &[("CacheClusterId", "delete-rg-cluster")],
    );
    service.delete_cache_cluster(&req).await.unwrap();

    let group = service
        .state
        .read()
        .default_ref()
        .replication_groups
        .get("delete-rg")
        .unwrap()
        .clone();
    assert_eq!(group.member_clusters, vec!["delete-rg-001"]);
    assert_eq!(group.num_cache_clusters, 1);
}

#[tokio::test]
async fn create_snapshot_rejects_standalone_cache_cluster_id() {
    let service = service_with_cache_cluster("standalone");
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "standalone-snap"),
            ("CacheClusterId", "standalone"),
        ],
    );
    assert!(service.create_snapshot(&req).await.is_err());
}

fn service_with_replication_group(group_id: &str, num_clusters: i32) -> ElastiCacheService {
    let shared: SharedElastiCacheState = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut __a = shared.write();
        let s = __a.default_mut();
        let member_clusters: Vec<String> = (1..=num_clusters)
            .map(|i| format!("{group_id}-{i:03}"))
            .collect();
        let arn = format!("arn:aws:elasticache:us-east-1:123456789012:replicationgroup:{group_id}");
        s.tags.insert(arn.clone(), Vec::new());
        s.replication_groups.insert(
            group_id.to_string(),
            ReplicationGroup {
                replication_group_id: group_id.to_string(),
                description: "test group".to_string(),
                global_replication_group_id: None,
                global_replication_group_role: None,
                status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                num_cache_clusters: num_clusters,
                automatic_failover_enabled: false,
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6379,
                arn,
                created_at: "2024-01-01T00:00:00Z".to_string(),
                container_id: "abc123".to_string(),
                host_port: 6379,
                member_clusters,
                snapshot_retention_limit: 0,
                snapshot_window: "05:00-09:00".to_string(),
                transit_encryption_enabled: false,
                at_rest_encryption_enabled: false,
                cluster_enabled: false,
                kms_key_id: None,
                auth_token_enabled: false,
                user_group_ids: Vec::new(),
                multi_az_enabled: false,
                log_delivery_configurations: Vec::new(),
                data_tiering: None,
                ip_discovery: None,
                network_type: None,
                transit_encryption_mode: None,
                num_node_groups: 1,
                configuration_endpoint_address: None,
                configuration_endpoint_port: None,
                replicas_per_node_group: None,
                auth_token: None,
                port: 6379,
                notification_topic_arn: None,
                cluster_mode: None,
                data_tiering_enabled: None,
                notification_topic_status: None,
                cache_parameter_group_name: None,
                cache_subnet_group_name: None,
                security_group_ids: Vec::new(),
                preferred_maintenance_window: None,
                snapshot_name: None,
                snapshot_arns: Vec::new(),
                auto_minor_version_upgrade: true,
            },
        );
    }
    ElastiCacheService::new(shared)
}

fn service_with_serverless_cache(cache_name: &str) -> ElastiCacheService {
    let shared: SharedElastiCacheState = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut __a = shared.write();
        let s = __a.default_mut();
        let arn =
            format!("arn:aws:elasticache:us-east-1:123456789012:serverlesscache:{cache_name}");
        s.tags.insert(arn.clone(), Vec::new());
        s.serverless_caches.insert(
            cache_name.to_string(),
            ServerlessCache {
                serverless_cache_name: cache_name.to_string(),
                description: "serverless cache".to_string(),
                engine: "redis".to_string(),
                major_engine_version: "7.1".to_string(),
                full_engine_version: "7.1".to_string(),
                status: "available".to_string(),
                endpoint: ServerlessCacheEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 6379,
                },
                reader_endpoint: ServerlessCacheEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 6379,
                },
                arn,
                created_at: "2024-01-01T00:00:00Z".to_string(),
                cache_usage_limits: Some(ServerlessCacheUsageLimits {
                    data_storage: Some(ServerlessCacheDataStorage {
                        maximum: Some(10),
                        minimum: Some(1),
                        unit: Some("GB".to_string()),
                    }),
                    ecpu_per_second: Some(ServerlessCacheEcpuPerSecond {
                        maximum: Some(5000),
                        minimum: Some(1000),
                    }),
                }),
                security_group_ids: vec!["sg-123".to_string()],
                subnet_ids: vec!["subnet-123".to_string()],
                kms_key_id: Some("kms-123".to_string()),
                user_group_id: None,
                snapshot_retention_limit: Some(1),
                daily_snapshot_time: Some("03:00".to_string()),
                container_id: "cid".to_string(),
                host_port: 6379,
            },
        );
    }
    ElastiCacheService::new(shared)
}

fn service_with_global_replication_group(
    global_replication_group_id: &str,
    replication_group_id: &str,
) -> ElastiCacheService {
    let service = service_with_replication_group(replication_group_id, 1);
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        state
            .replication_groups
            .get_mut(replication_group_id)
            .unwrap()
            .global_replication_group_id = Some(global_replication_group_id.to_string());
        state
            .replication_groups
            .get_mut(replication_group_id)
            .unwrap()
            .global_replication_group_role = Some("primary".to_string());
        state.global_replication_groups.insert(
            global_replication_group_id.to_string(),
            GlobalReplicationGroup {
                global_replication_group_id: global_replication_group_id.to_string(),
                global_replication_group_description: "global test group".to_string(),
                status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                members: vec![GlobalReplicationGroupMember {
                    replication_group_id: replication_group_id.to_string(),
                    replication_group_region: "us-east-1".to_string(),
                    role: "primary".to_string(),
                    automatic_failover: false,
                    status: "associated".to_string(),
                }],
                cluster_enabled: true,
                arn: format!(
                    "arn:aws:elasticache:us-east-1:123456789012:globalreplicationgroup:{global_replication_group_id}"
                ),
                num_node_groups: 2,
            },
        );
    }
    service
}

#[test]
fn create_global_replication_group_registers_metadata_and_updates_primary_group() {
    let service = service_with_replication_group("primary-rg", 1);
    let req = request(
        "CreateGlobalReplicationGroup",
        &[
            ("GlobalReplicationGroupIdSuffix", "global-a"),
            ("PrimaryReplicationGroupId", "primary-rg"),
            ("GlobalReplicationGroupDescription", "global slice"),
        ],
    );

    let resp = service.create_global_replication_group(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains(
        "<GlobalReplicationGroupDescription>global slice</GlobalReplicationGroupDescription>"
    ));
    assert!(body.contains("<ReplicationGroupId>primary-rg</ReplicationGroupId>"));
    assert!(body.contains("<Role>primary</Role>"));

    let __a = service.state.read();
    let state = __a.default_ref();
    let primary_group = state.replication_groups.get("primary-rg").unwrap();
    assert_eq!(
        primary_group.global_replication_group_id.as_deref(),
        Some("fc-us-east-1-global-a")
    );
    assert_eq!(
        primary_group.global_replication_group_role.as_deref(),
        Some("primary")
    );
    assert!(state
        .global_replication_groups
        .contains_key("fc-us-east-1-global-a"));
}

#[test]
fn describe_global_replication_groups_filters_by_id() {
    let service = service_with_global_replication_group("fc-us-east-1-global-a", "primary-rg");
    let req = request(
        "DescribeGlobalReplicationGroups",
        &[
            ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
            ("ShowMemberInfo", "true"),
        ],
    );

    let resp = service.describe_global_replication_groups(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        body.contains("<GlobalReplicationGroupId>fc-us-east-1-global-a</GlobalReplicationGroupId>")
    );
    assert!(body.contains("<ReplicationGroupId>primary-rg</ReplicationGroupId>"));
    assert!(body.contains("<DescribeGlobalReplicationGroupsResponse"));
}

/// 1.23: Increase/Decrease node groups in a global replication group must
/// actually apply the count and reflect it in
/// DescribeGlobalReplicationGroups (no longer a no-op).
#[test]
fn global_node_group_reshard_applies_and_reflects() {
    let service = service_with_global_replication_group("fc-us-east-1-global-a", "primary-rg");

    let count_node_groups = |service: &ElastiCacheService| -> usize {
        let resp = service
            .describe_global_replication_groups(&request(
                "DescribeGlobalReplicationGroups",
                &[
                    ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
                    ("ShowMemberInfo", "true"),
                ],
            ))
            .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        body.matches("<GlobalNodeGroup>").count()
    };

    // Seed starts at 2 node groups.
    assert_eq!(count_node_groups(&service), 2);

    // Increase to 4.
    service
        .increase_node_groups_in_global_replication_group(&request(
            "IncreaseNodeGroupsInGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
                ("NodeGroupCount", "4"),
                ("ApplyImmediately", "true"),
            ],
        ))
        .unwrap();
    assert_eq!(count_node_groups(&service), 4, "increase must apply");

    // Decrease to 1.
    service
        .decrease_node_groups_in_global_replication_group(&request(
            "DecreaseNodeGroupsInGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
                ("NodeGroupCount", "1"),
                ("ApplyImmediately", "true"),
            ],
        ))
        .unwrap();
    assert_eq!(count_node_groups(&service), 1, "decrease must apply");

    // Increase to a value not greater than current is rejected.
    let err = service
        .increase_node_groups_in_global_replication_group(&request(
            "IncreaseNodeGroupsInGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
                ("NodeGroupCount", "1"),
            ],
        ))
        .err()
        .expect("non-increasing count rejected");
    assert!(format!("{err:?}").contains("InvalidParameterValueException"));
}

#[test]
fn modify_global_replication_group_updates_primary_replication_group_state() {
    let service = service_with_global_replication_group("fc-us-east-1-global-a", "primary-rg");
    let req = request(
        "ModifyGlobalReplicationGroup",
        &[
            ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
            ("ApplyImmediately", "true"),
            ("GlobalReplicationGroupDescription", "updated"),
            ("CacheNodeType", "cache.m5.large"),
            ("EngineVersion", "7.2"),
            ("AutomaticFailoverEnabled", "true"),
        ],
    );

    let resp = service.modify_global_replication_group(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains(
        "<GlobalReplicationGroupDescription>updated</GlobalReplicationGroupDescription>"
    ));
    assert!(body.contains("<CacheNodeType>cache.m5.large</CacheNodeType>"));
    assert!(body.contains("<EngineVersion>7.2</EngineVersion>"));

    let __a = service.state.read();
    let state = __a.default_ref();
    let primary_group = state.replication_groups.get("primary-rg").unwrap();
    assert_eq!(primary_group.cache_node_type, "cache.m5.large");
    assert_eq!(primary_group.engine_version, "7.2");
    assert!(primary_group.automatic_failover_enabled);
}

#[test]
fn delete_global_replication_group_clears_primary_group_association() {
    let service = service_with_global_replication_group("fc-us-east-1-global-a", "primary-rg");
    let req = request(
        "DeleteGlobalReplicationGroup",
        &[
            ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
            ("RetainPrimaryReplicationGroup", "true"),
        ],
    );

    let resp = service.delete_global_replication_group(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Status>deleting</Status>"));

    let __a = service.state.read();
    let state = __a.default_ref();
    assert!(!state
        .global_replication_groups
        .contains_key("fc-us-east-1-global-a"));
    let primary_group = state.replication_groups.get("primary-rg").unwrap();
    assert!(primary_group.global_replication_group_id.is_none());
    assert!(primary_group.global_replication_group_role.is_none());
}

#[test]
fn replication_group_xml_emits_dynamic_encryption_and_kms() {
    let mut state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
    state.replication_groups.insert(
        "enc-rg".to_string(),
        ReplicationGroup {
            replication_group_id: "enc-rg".to_string(),
            description: "encrypted".to_string(),
            global_replication_group_id: None,
            global_replication_group_role: None,
            status: "available".to_string(),
            cache_node_type: "cache.t3.micro".to_string(),
            engine: "redis".to_string(),
            engine_version: "7.1".to_string(),
            num_cache_clusters: 1,
            automatic_failover_enabled: true,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: 6379,
            arn: "arn:aws:elasticache:us-east-1:123:replicationgroup:enc-rg".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            container_id: "c".to_string(),
            host_port: 6379,
            member_clusters: vec!["enc-rg-001".to_string()],
            snapshot_retention_limit: 5,
            snapshot_window: "05:00-09:00".to_string(),
            transit_encryption_enabled: true,
            at_rest_encryption_enabled: true,
            cluster_enabled: true,
            kms_key_id: Some("arn:aws:kms:us-east-1:123:key/abc-123".to_string()),
            auth_token_enabled: true,
            user_group_ids: vec!["ug-prod".to_string()],
            multi_az_enabled: true,
            log_delivery_configurations: vec![crate::state::LogDeliveryConfiguration {
                log_type: "slow-log".to_string(),
                destination_type: "cloudwatch-logs".to_string(),
                destination_details: Some("/aws/elasticache/slow".to_string()),
                log_format: "json".to_string(),
                status: "active".to_string(),
            }],
            data_tiering: Some("disabled".to_string()),
            ip_discovery: Some("ipv4".to_string()),
            network_type: Some("dual_stack".to_string()),
            transit_encryption_mode: Some("required".to_string()),
            num_node_groups: 2,
            configuration_endpoint_address: Some("config.local".to_string()),
            configuration_endpoint_port: Some(6379),
            replicas_per_node_group: Some(1),
            auth_token: Some("supersecret".to_string()),
            port: 6379,
            notification_topic_arn: Some("arn:aws:sns:us-east-1:123:topic-a".to_string()),
            cluster_mode: Some("enabled".to_string()),
            data_tiering_enabled: Some(false),
            notification_topic_status: None,
            cache_parameter_group_name: Some("default.redis7".to_string()),
            cache_subnet_group_name: Some("default".to_string()),
            security_group_ids: vec!["sg-aaaa".to_string()],
            preferred_maintenance_window: Some("sun:23:00-mon:01:30".to_string()),
            snapshot_name: None,
            snapshot_arns: Vec::new(),
            auto_minor_version_upgrade: true,
        },
    );
    let xml =
        super::replication_group_xml(state.replication_groups.get("enc-rg").unwrap(), "us-east-1");
    assert!(xml.contains("<TransitEncryptionEnabled>true</TransitEncryptionEnabled>"));
    assert!(xml.contains("<AtRestEncryptionEnabled>true</AtRestEncryptionEnabled>"));
    assert!(xml.contains("<ClusterEnabled>true</ClusterEnabled>"));
    assert!(xml.contains("<KmsKeyId>arn:aws:kms:us-east-1:123:key/abc-123</KmsKeyId>"));
    assert!(xml.contains("<AuthTokenEnabled>true</AuthTokenEnabled>"));
    assert!(xml.contains("<MultiAZ>enabled</MultiAZ>"));
    assert!(xml.contains("<UserGroupIds><member>ug-prod</member></UserGroupIds>"));
    assert!(xml.contains("<LogDeliveryConfigurations>"));
    assert!(xml.contains("<DataTiering>disabled</DataTiering>"));
    assert!(xml.contains("<NetworkType>dual_stack</NetworkType>"));
    assert!(xml.contains("<TransitEncryptionMode>required</TransitEncryptionMode>"));
    assert!(xml.contains("<ConfigurationEndpoint>"));
    assert!(xml
        .contains("<ReplicationGroupCreateTime>2024-01-01T00:00:00Z</ReplicationGroupCreateTime>"));
}

#[test]
fn replication_group_xml_includes_global_replication_group_info() {
    let service = service_with_global_replication_group("fc-us-east-1-global-a", "primary-rg");
    let req = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "primary-rg")],
    );

    let resp = service.describe_replication_groups(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<GlobalReplicationGroupInfo>"));
    assert!(
        body.contains("<GlobalReplicationGroupId>fc-us-east-1-global-a</GlobalReplicationGroupId>")
    );
    assert!(body
        .contains("<GlobalReplicationGroupMemberRole>primary</GlobalReplicationGroupMemberRole>"));
}

#[test]
fn failover_global_replication_group_returns_current_primary() {
    let service = service_with_global_replication_group("fc-us-east-1-global-a", "primary-rg");
    let req = request(
        "FailoverGlobalReplicationGroup",
        &[
            ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
            ("PrimaryRegion", "us-east-1"),
            ("PrimaryReplicationGroupId", "primary-rg"),
        ],
    );

    let resp = service.failover_global_replication_group(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ReplicationGroupId>primary-rg</ReplicationGroupId>"));
    assert!(body.contains("<FailoverGlobalReplicationGroupResponse"));
}

#[test]
fn disassociate_global_replication_group_accepts_current_primary_as_noop() {
    let service = service_with_global_replication_group("fc-us-east-1-global-a", "primary-rg");
    let req = request(
        "DisassociateGlobalReplicationGroup",
        &[
            ("GlobalReplicationGroupId", "fc-us-east-1-global-a"),
            ("ReplicationGroupId", "primary-rg"),
            ("ReplicationGroupRegion", "us-east-1"),
        ],
    );

    let resp = service.disassociate_global_replication_group(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ReplicationGroupId>primary-rg</ReplicationGroupId>"));
    assert!(body.contains("<DisassociateGlobalReplicationGroupResponse"));
}

#[tokio::test]
async fn modify_replication_group_updates_description() {
    let service = service_with_replication_group("my-rg", 1);
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "my-rg"),
            ("ReplicationGroupDescription", "Updated description"),
        ],
    );
    let resp = service.modify_replication_group(&req).await.unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Description>Updated description</Description>"));
    assert!(body.contains("<ModifyReplicationGroupResponse"));
}

#[tokio::test]
async fn modify_replication_group_updates_multiple_fields() {
    let service = service_with_replication_group("my-rg", 1);
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "my-rg"),
            ("CacheNodeType", "cache.m5.large"),
            ("AutomaticFailoverEnabled", "true"),
            ("SnapshotRetentionLimit", "5"),
            ("SnapshotWindow", "02:00-06:00"),
        ],
    );
    let resp = service.modify_replication_group(&req).await.unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheNodeType>cache.m5.large</CacheNodeType>"));
    assert!(body.contains("<AutomaticFailover>enabled</AutomaticFailover>"));
    assert!(body.contains("<SnapshotRetentionLimit>5</SnapshotRetentionLimit>"));
    assert!(body.contains("<SnapshotWindow>02:00-06:00</SnapshotWindow>"));
}

#[tokio::test]
async fn modify_replication_group_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    let req = request(
        "ModifyReplicationGroup",
        &[("ReplicationGroupId", "nonexistent")],
    );
    assert!(service.modify_replication_group(&req).await.is_err());
}

#[tokio::test]
async fn modify_replication_group_updates_auth_token_with_set() {
    let service = service_with_replication_group("rg-auth", 1);
    {
        let mut a = service.state.write();
        let g = a
            .default_mut()
            .replication_groups
            .get_mut("rg-auth")
            .unwrap();
        g.auth_token = Some("a".to_string());
        g.auth_token_enabled = true;
    }
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-auth"),
            ("AuthToken", "b"),
            ("AuthTokenUpdateStrategy", "SET"),
        ],
    );
    let resp = service.modify_replication_group(&req).await.unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    // Token is never echoed back in the XML payload.
    assert!(!body.contains("<AuthToken>"));
    assert!(body.contains("<AuthTokenEnabled>true</AuthTokenEnabled>"));

    let a = service.state.read();
    let g = a.default_ref().replication_groups.get("rg-auth").unwrap();
    assert_eq!(g.auth_token.as_deref(), Some("b"));
    assert!(g.auth_token_enabled);
}

#[tokio::test]
async fn modify_replication_group_delete_auth_token_strategy_clears_token() {
    let service = service_with_replication_group("rg-del", 1);
    {
        let mut a = service.state.write();
        let g = a
            .default_mut()
            .replication_groups
            .get_mut("rg-del")
            .unwrap();
        g.auth_token = Some("secret".to_string());
        g.auth_token_enabled = true;
    }
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-del"),
            ("AuthTokenUpdateStrategy", "DELETE"),
        ],
    );
    service.modify_replication_group(&req).await.unwrap();

    let a = service.state.read();
    let g = a.default_ref().replication_groups.get("rg-del").unwrap();
    assert!(g.auth_token.is_none());
    assert!(!g.auth_token_enabled);
}

#[tokio::test]
async fn modify_replication_group_remove_user_groups_clears_list() {
    let service = service_with_replication_group("rg-ug", 1);
    {
        let mut a = service.state.write();
        let g = a.default_mut().replication_groups.get_mut("rg-ug").unwrap();
        g.user_group_ids = vec!["a".to_string(), "b".to_string()];
    }
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-ug"),
            ("RemoveUserGroups", "true"),
        ],
    );
    service.modify_replication_group(&req).await.unwrap();

    let a = service.state.read();
    let g = a.default_ref().replication_groups.get("rg-ug").unwrap();
    assert!(g.user_group_ids.is_empty());
}

#[tokio::test]
async fn modify_replication_group_persists_log_delivery_changes() {
    let service = service_with_replication_group("rg-log", 1);
    {
        let mut a = service.state.write();
        let g = a
            .default_mut()
            .replication_groups
            .get_mut("rg-log")
            .unwrap();
        g.log_delivery_configurations = vec![crate::state::LogDeliveryConfiguration {
            log_type: "slow-log".to_string(),
            destination_type: "cloudwatch-logs".to_string(),
            destination_details: Some("/aws/orig".to_string()),
            log_format: "json".to_string(),
            status: "active".to_string(),
        }];
    }
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-log"),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogType",
                "slow-log",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationType",
                "cloudwatch-logs",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationDetails.CloudWatchLogsDetails.LogGroup",
                "/aws/new-slow",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogFormat",
                "json",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.LogType",
                "engine-log",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.DestinationType",
                "kinesis-firehose",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.DestinationDetails.KinesisFirehoseDetails.DeliveryStream",
                "engine-stream",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.LogFormat",
                "text",
            ),
        ],
    );
    service.modify_replication_group(&req).await.unwrap();

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "rg-log")],
    );
    let resp = service.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("/aws/new-slow"));
    assert!(body.contains("engine-stream"));
    assert!(!body.contains("/aws/orig"));

    let a = service.state.read();
    let g = a.default_ref().replication_groups.get("rg-log").unwrap();
    assert_eq!(g.log_delivery_configurations.len(), 2);
}

#[tokio::test]
async fn modify_replication_group_persists_multi_az_and_network_fields() {
    let service = service_with_replication_group("rg-net", 1);
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-net"),
            ("MultiAZEnabled", "true"),
            ("IpDiscovery", "ipv6"),
            ("NetworkType", "dual_stack"),
        ],
    );
    service.modify_replication_group(&req).await.unwrap();

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "rg-net")],
    );
    let resp = service.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<MultiAZ>enabled</MultiAZ>"));
    assert!(body.contains("<IpDiscovery>ipv6</IpDiscovery>"));
    assert!(body.contains("<NetworkType>dual_stack</NetworkType>"));
}

#[tokio::test]
async fn modify_replication_group_persists_snapshot_retention_and_window() {
    let service = service_with_replication_group("rg-snap", 1);
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-snap"),
            ("SnapshotRetentionLimit", "14"),
            ("SnapshotWindow", "01:00-03:00"),
        ],
    );
    service.modify_replication_group(&req).await.unwrap();

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "rg-snap")],
    );
    let resp = service.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<SnapshotRetentionLimit>14</SnapshotRetentionLimit>"));
    assert!(body.contains("<SnapshotWindow>01:00-03:00</SnapshotWindow>"));
}

#[tokio::test]
async fn modify_replication_group_persists_notification_topic_attrs() {
    let service = service_with_replication_group("rg-notif", 1);
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:elasticache-events";
    let req = request(
        "ModifyReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-notif"),
            ("NotificationTopicArn", topic_arn),
            ("NotificationTopicStatus", "inactive"),
        ],
    );
    service.modify_replication_group(&req).await.unwrap();

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "rg-notif")],
    );
    let resp = service.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains(&format!("<TopicArn>{topic_arn}</TopicArn>")));
    assert!(body.contains("<TopicStatus>inactive</TopicStatus>"));
}

#[test]
fn parse_cache_usage_limits_reads_nested_query_shape() {
    let req = request(
        "CreateServerlessCache",
        &[
            ("CacheUsageLimits.DataStorage.Maximum", "10"),
            ("CacheUsageLimits.DataStorage.Minimum", "2"),
            ("CacheUsageLimits.DataStorage.Unit", "GB"),
            ("CacheUsageLimits.ECPUPerSecond.Maximum", "5000"),
            ("CacheUsageLimits.ECPUPerSecond.Minimum", "1000"),
        ],
    );

    let limits = parse_cache_usage_limits(&req).unwrap().unwrap();
    let data_storage = limits.data_storage.unwrap();
    assert_eq!(data_storage.maximum, Some(10));
    assert_eq!(data_storage.minimum, Some(2));
    assert_eq!(data_storage.unit.as_deref(), Some("GB"));

    let ecpu = limits.ecpu_per_second.unwrap();
    assert_eq!(ecpu.maximum, Some(5000));
    assert_eq!(ecpu.minimum, Some(1000));
}

#[test]
fn serverless_cache_xml_contains_expected_fields() {
    let cache = service_with_serverless_cache("cache-a")
        .state
        .read()
        .default_ref()
        .serverless_caches["cache-a"]
        .clone();

    let xml = serverless_cache_xml(&cache);
    assert!(xml.contains("<ServerlessCacheName>cache-a</ServerlessCacheName>"));
    assert!(xml.contains("<Engine>redis</Engine>"));
    assert!(xml.contains("<MajorEngineVersion>7.1</MajorEngineVersion>"));
    assert!(xml.contains("<Endpoint><Address>127.0.0.1</Address><Port>6379</Port></Endpoint>"));
    assert!(xml.contains(
        "<ReaderEndpoint><Address>127.0.0.1</Address><Port>6379</Port></ReaderEndpoint>"
    ));
    assert!(xml.contains(
        "<SecurityGroupIds><SecurityGroupId>sg-123</SecurityGroupId></SecurityGroupIds>"
    ));
    assert!(xml.contains("<SubnetIds><member>subnet-123</member></SubnetIds>"));
    assert!(xml.contains("<CacheUsageLimits>"));
}

#[test]
fn serverless_cache_snapshot_xml_contains_expected_fields() {
    let snapshot = ServerlessCacheSnapshot {
        serverless_cache_snapshot_name: "snap-a".to_string(),
        arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a".to_string(),
        kms_key_id: Some("kms-123".to_string()),
        snapshot_type: "manual".to_string(),
        status: "available".to_string(),
        create_time: "2024-01-01T00:00:00Z".to_string(),
        expiry_time: None,
        bytes_used_for_cache: Some("0".to_string()),
        serverless_cache_name: "cache-a".to_string(),
        engine: "redis".to_string(),
        major_engine_version: "7.1".to_string(),
    };

    let xml = serverless_cache_snapshot_xml(&snapshot);
    assert!(xml.contains("<ServerlessCacheSnapshotName>snap-a</ServerlessCacheSnapshotName>"));
    assert!(xml.contains("<KmsKeyId>kms-123</KmsKeyId>"));
    assert!(xml.contains("<SnapshotType>manual</SnapshotType>"));
    assert!(xml.contains("<ServerlessCacheConfiguration>"));
    assert!(xml.contains("<ServerlessCacheName>cache-a</ServerlessCacheName>"));
}

#[test]
fn describe_serverless_caches_returns_all() {
    let service = service_with_serverless_cache("cache-a");
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        state.serverless_caches.insert(
            "cache-b".to_string(),
            ServerlessCache {
                serverless_cache_name: "cache-b".to_string(),
                description: "serverless cache".to_string(),
                engine: "valkey".to_string(),
                major_engine_version: "8.0".to_string(),
                full_engine_version: "8.0".to_string(),
                status: "available".to_string(),
                endpoint: ServerlessCacheEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 6380,
                },
                reader_endpoint: ServerlessCacheEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 6380,
                },
                arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:cache-b"
                    .to_string(),
                created_at: "2024-01-02T00:00:00Z".to_string(),
                cache_usage_limits: None,
                security_group_ids: Vec::new(),
                subnet_ids: Vec::new(),
                kms_key_id: None,
                user_group_id: None,
                snapshot_retention_limit: None,
                daily_snapshot_time: None,
                container_id: "cid".to_string(),
                host_port: 6380,
            },
        );
    }

    let resp = service
        .describe_serverless_caches(&request("DescribeServerlessCaches", &[]))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ServerlessCacheName>cache-a</ServerlessCacheName>"));
    assert!(body.contains("<ServerlessCacheName>cache-b</ServerlessCacheName>"));
}

#[test]
fn modify_serverless_cache_updates_fields() {
    let service = service_with_serverless_cache("cache-a");
    let req = request(
        "ModifyServerlessCache",
        &[
            ("ServerlessCacheName", "cache-a"),
            ("Description", "updated"),
            ("SecurityGroupIds.SecurityGroupId.1", "sg-999"),
            ("SnapshotRetentionLimit", "7"),
            ("DailySnapshotTime", "05:00"),
        ],
    );

    let resp = service.modify_serverless_cache(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Description>updated</Description>"));
    assert!(body.contains(
        "<SecurityGroupIds><SecurityGroupId>sg-999</SecurityGroupId></SecurityGroupIds>"
    ));
    assert!(body.contains("<SnapshotRetentionLimit>7</SnapshotRetentionLimit>"));
    assert!(body.contains("<DailySnapshotTime>05:00</DailySnapshotTime>"));
}

#[test]
fn parse_query_list_param_reads_indexed_and_flat_query_values() {
    let req = AwsRequest {
        service: "elasticache".to_string(),
        action: "ModifyServerlessCache".to_string(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-1".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::from([
            ("SecurityGroupIds.member.1".to_string(), "sg-a".to_string()),
            ("SecurityGroupIds.member.2".to_string(), "sg-b".to_string()),
        ]),
        body: Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    };
    assert_eq!(
        parse_query_list_param(&req, "SecurityGroupIds", "SecurityGroupId"),
        vec!["sg-a".to_string(), "sg-b".to_string()]
    );

    let req = AwsRequest {
        query_params: HashMap::from([("SecurityGroupIds".to_string(), "sg-flat".to_string())]),
        ..req
    };
    assert_eq!(
        parse_query_list_param(&req, "SecurityGroupIds", "SecurityGroupId"),
        vec!["sg-flat".to_string()]
    );
}

#[test]
fn describe_serverless_cache_snapshots_filters_by_cache_name() {
    let service = service_with_serverless_cache("cache-a");
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        state.serverless_cache_snapshots.insert(
            "snap-a".to_string(),
            ServerlessCacheSnapshot {
                serverless_cache_snapshot_name: "snap-a".to_string(),
                arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a"
                    .to_string(),
                kms_key_id: None,
                snapshot_type: "manual".to_string(),
                status: "available".to_string(),
                create_time: "2024-01-01T00:00:00Z".to_string(),
                expiry_time: None,
                bytes_used_for_cache: None,
                serverless_cache_name: "cache-a".to_string(),
                engine: "redis".to_string(),
                major_engine_version: "7.1".to_string(),
            },
        );
    }

    let resp = service
        .describe_serverless_cache_snapshots(&request(
            "DescribeServerlessCacheSnapshots",
            &[("ServerlessCacheName", "cache-a")],
        ))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ServerlessCacheSnapshotName>snap-a</ServerlessCacheSnapshotName>"));
}

#[test]
fn delete_serverless_cache_snapshot_removes_tags() {
    let service = service_with_serverless_cache("cache-a");
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        let arn =
            "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a".to_string();
        state.tags.insert(arn.clone(), Vec::new());
        state.serverless_cache_snapshots.insert(
            "snap-a".to_string(),
            ServerlessCacheSnapshot {
                serverless_cache_snapshot_name: "snap-a".to_string(),
                arn,
                kms_key_id: None,
                snapshot_type: "manual".to_string(),
                status: "available".to_string(),
                create_time: "2024-01-01T00:00:00Z".to_string(),
                expiry_time: None,
                bytes_used_for_cache: None,
                serverless_cache_name: "cache-a".to_string(),
                engine: "redis".to_string(),
                major_engine_version: "7.1".to_string(),
            },
        );
    }

    let resp = service
        .delete_serverless_cache_snapshot(&request(
            "DeleteServerlessCacheSnapshot",
            &[("ServerlessCacheSnapshotName", "snap-a")],
        ))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Status>deleting</Status>"));
    assert!(!service
        .state
        .read()
        .default_ref()
        .tags
        .contains_key("arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a"));
}

#[test]
fn increase_replica_count_updates_member_clusters() {
    let service = service_with_replication_group("my-rg", 1);
    let req = request(
        "IncreaseReplicaCount",
        &[
            ("ReplicationGroupId", "my-rg"),
            ("ApplyImmediately", "true"),
            ("NewReplicaCount", "2"),
        ],
    );
    let resp = service.increase_replica_count(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ClusterId>my-rg-001</ClusterId>"));
    assert!(body.contains("<ClusterId>my-rg-002</ClusterId>"));
    assert!(body.contains("<ClusterId>my-rg-003</ClusterId>"));
    assert!(body.contains("<IncreaseReplicaCountResponse"));
}

#[test]
fn increase_replica_count_rejects_same_or_lower() {
    let service = service_with_replication_group("my-rg", 3);
    let req = request(
        "IncreaseReplicaCount",
        &[
            ("ReplicationGroupId", "my-rg"),
            ("ApplyImmediately", "true"),
            ("NewReplicaCount", "2"),
        ],
    );
    assert!(service.increase_replica_count(&req).is_err());
}

#[test]
fn increase_replica_count_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    let req = request(
        "IncreaseReplicaCount",
        &[
            ("ReplicationGroupId", "nonexistent"),
            ("ApplyImmediately", "true"),
            ("NewReplicaCount", "2"),
        ],
    );
    assert!(service.increase_replica_count(&req).is_err());
}

#[test]
fn decrease_replica_count_updates_member_clusters() {
    let service = service_with_replication_group("my-rg", 3);
    let req = request(
        "DecreaseReplicaCount",
        &[
            ("ReplicationGroupId", "my-rg"),
            ("ApplyImmediately", "true"),
            ("NewReplicaCount", "1"),
        ],
    );
    let resp = service.decrease_replica_count(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ClusterId>my-rg-001</ClusterId>"));
    assert!(body.contains("<ClusterId>my-rg-002</ClusterId>"));
    assert!(!body.contains("<ClusterId>my-rg-003</ClusterId>"));
    assert!(body.contains("<DecreaseReplicaCountResponse"));
}

#[test]
fn decrease_replica_count_validates_minimum() {
    let service = service_with_replication_group("my-rg", 1);
    // NewReplicaCount=0 means total=1, which is not fewer than current 1
    let req = request(
        "DecreaseReplicaCount",
        &[
            ("ReplicationGroupId", "my-rg"),
            ("ApplyImmediately", "true"),
            ("NewReplicaCount", "0"),
        ],
    );
    assert!(service.decrease_replica_count(&req).is_err());
}

#[test]
fn decrease_replica_count_rejects_negative() {
    let service = service_with_replication_group("my-rg", 2);
    let req = request(
        "DecreaseReplicaCount",
        &[
            ("ReplicationGroupId", "my-rg"),
            ("ApplyImmediately", "true"),
            ("NewReplicaCount", "-1"),
        ],
    );
    assert!(service.decrease_replica_count(&req).is_err());
}

#[test]
fn modify_replication_group_shard_configuration_changes_shard_count() {
    // Cluster-mode replication group, 2 shards -> 4 shards. Asserts the
    // server actually mutates `num_node_groups` and rebuilds member_clusters,
    // not the legacy echo behaviour.
    let service = service_with_replication_group("shard-rg", 1);
    {
        let mut accounts = service.state.write();
        let g = accounts
            .default_mut()
            .replication_groups
            .get_mut("shard-rg")
            .unwrap();
        g.num_node_groups = 2;
        g.cluster_enabled = true;
        g.replicas_per_node_group = Some(1);
    }
    let req = request(
        "ModifyReplicationGroupShardConfiguration",
        &[
            ("ReplicationGroupId", "shard-rg"),
            ("NodeGroupCount", "4"),
            ("ApplyImmediately", "true"),
        ],
    );
    let resp = service
        .modify_replication_group_shard_configuration(&req)
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    // 4 shards * (1+1) = 8 member clusters in the rebuilt list.
    assert!(body.contains("<ClusterId>shard-rg-008</ClusterId>"));
    assert!(!body.contains("<ClusterId>shard-rg-009</ClusterId>"));
    // 4 NodeGroup XML elements.
    assert_eq!(body.matches("<NodeGroup>").count(), 4);
}

#[test]
fn modify_replication_group_shard_configuration_rejects_non_cluster_change() {
    let service = service_with_replication_group("solo-rg", 1);
    let req = request(
        "ModifyReplicationGroupShardConfiguration",
        &[
            ("ReplicationGroupId", "solo-rg"),
            ("NodeGroupCount", "3"),
            ("ApplyImmediately", "true"),
        ],
    );
    assert!(
        service
            .modify_replication_group_shard_configuration(&req)
            .is_err(),
        "non-cluster replication group must reject NodeGroupCount changes"
    );
}

#[test]
fn modify_replication_group_shard_configuration_decrease_requires_remove_or_retain() {
    let service = service_with_replication_group("dec-rg", 1);
    {
        let mut accounts = service.state.write();
        let g = accounts
            .default_mut()
            .replication_groups
            .get_mut("dec-rg")
            .unwrap();
        g.num_node_groups = 3;
        g.cluster_enabled = true;
    }
    let req = request(
        "ModifyReplicationGroupShardConfiguration",
        &[
            ("ReplicationGroupId", "dec-rg"),
            ("NodeGroupCount", "1"),
            ("ApplyImmediately", "true"),
        ],
    );
    assert!(
        service
            .modify_replication_group_shard_configuration(&req)
            .is_err(),
        "decrease without NodeGroupsToRetain/Remove must fail"
    );
}

#[test]
fn increase_replica_count_supports_replica_configuration() {
    let service = service_with_replication_group("conf-rg", 1);
    {
        let mut accounts = service.state.write();
        let g = accounts
            .default_mut()
            .replication_groups
            .get_mut("conf-rg")
            .unwrap();
        g.num_node_groups = 2;
        g.cluster_enabled = true;
        g.replicas_per_node_group = Some(0);
    }
    let req = request(
        "IncreaseReplicaCount",
        &[
            ("ReplicationGroupId", "conf-rg"),
            ("ApplyImmediately", "true"),
            ("ReplicaConfiguration.ConfigureShard.1.NodeGroupId", "0001"),
            ("ReplicaConfiguration.ConfigureShard.1.NewReplicaCount", "2"),
            ("ReplicaConfiguration.ConfigureShard.2.NodeGroupId", "0002"),
            ("ReplicaConfiguration.ConfigureShard.2.NewReplicaCount", "2"),
        ],
    );
    let resp = service.increase_replica_count(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    // 2 shards × (1 + 2 replicas) = 6 member clusters.
    assert!(body.contains("<ClusterId>conf-rg-006</ClusterId>"));
    assert!(!body.contains("<ClusterId>conf-rg-007</ClusterId>"));
}

#[test]
fn test_failover_validates_node_group() {
    let service = service_with_replication_group("my-rg", 1);
    let req = request(
        "TestFailover",
        &[("ReplicationGroupId", "my-rg"), ("NodeGroupId", "0001")],
    );
    let resp = service.test_failover(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Status>available</Status>"));
    assert!(body.contains("<TestFailoverResponse"));
}

#[test]
fn test_failover_rejects_invalid_node_group() {
    let service = service_with_replication_group("my-rg", 1);
    let req = request(
        "TestFailover",
        &[("ReplicationGroupId", "my-rg"), ("NodeGroupId", "9999")],
    );
    assert!(service.test_failover(&req).is_err());
}

#[test]
fn test_failover_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    let req = request(
        "TestFailover",
        &[
            ("ReplicationGroupId", "nonexistent"),
            ("NodeGroupId", "0001"),
        ],
    );
    assert!(service.test_failover(&req).is_err());
}

// Snapshot tests

#[tokio::test]
async fn create_snapshot_returns_snapshot_xml() {
    let service = service_with_replication_group("snap-rg", 1);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "my-snap"),
            ("ReplicationGroupId", "snap-rg"),
        ],
    );
    let resp = service.create_snapshot(&req).await.unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<SnapshotName>my-snap</SnapshotName>"));
    assert!(body.contains("<ReplicationGroupId>snap-rg</ReplicationGroupId>"));
    assert!(body.contains("<SnapshotStatus>available</SnapshotStatus>"));
    assert!(body.contains("<SnapshotSource>manual</SnapshotSource>"));
    assert!(body.contains("<Engine>redis</Engine>"));
    assert!(body.contains("<CreateSnapshotResponse"));
}

#[tokio::test]
async fn create_snapshot_via_cache_cluster_id() {
    let service = service_with_replication_group("cc-rg", 2);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "cluster-snap"),
            ("CacheClusterId", "cc-rg-001"),
        ],
    );
    let resp = service.create_snapshot(&req).await.unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ReplicationGroupId>cc-rg</ReplicationGroupId>"));
}

#[tokio::test]
async fn create_snapshot_rejects_missing_group_and_cluster() {
    let service = service_with_replication_group("rg", 1);
    let req = request("CreateSnapshot", &[("SnapshotName", "bad-snap")]);
    assert!(service.create_snapshot(&req).await.is_err());
}

#[tokio::test]
async fn create_snapshot_rejects_duplicate_name() {
    let service = service_with_replication_group("dup-rg", 1);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "dup-snap"),
            ("ReplicationGroupId", "dup-rg"),
        ],
    );
    service.create_snapshot(&req).await.unwrap();
    assert!(service.create_snapshot(&req).await.is_err());
}

#[tokio::test]
async fn create_snapshot_rejects_nonexistent_group() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "orphan"),
            ("ReplicationGroupId", "no-such-rg"),
        ],
    );
    assert!(service.create_snapshot(&req).await.is_err());
}

#[tokio::test]
async fn create_snapshot_rejects_missing_name() {
    let service = service_with_replication_group("rg", 1);
    let req = request("CreateSnapshot", &[("ReplicationGroupId", "rg")]);
    assert!(service.create_snapshot(&req).await.is_err());
}

#[tokio::test]
async fn create_snapshot_registers_arn_for_tags() {
    let service = service_with_replication_group("tag-rg", 1);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "tag-snap"),
            ("ReplicationGroupId", "tag-rg"),
        ],
    );
    service.create_snapshot(&req).await.unwrap();

    let __a = service.state.read();
    let state = __a.default_ref();
    let arn = "arn:aws:elasticache:us-east-1:123456789012:snapshot:tag-snap".to_string();
    assert!(state.tags.contains_key(&arn));
}

#[tokio::test]
async fn describe_snapshots_returns_all() {
    let service = service_with_replication_group("desc-rg", 1);
    for name in &["snap-a", "snap-b"] {
        let req = request(
            "CreateSnapshot",
            &[("SnapshotName", name), ("ReplicationGroupId", "desc-rg")],
        );
        service.create_snapshot(&req).await.unwrap();
    }
    let req = request("DescribeSnapshots", &[]);
    let resp = service.describe_snapshots(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<SnapshotName>snap-a</SnapshotName>"));
    assert!(body.contains("<SnapshotName>snap-b</SnapshotName>"));
    assert!(body.contains("<DescribeSnapshotsResponse"));
}

#[tokio::test]
async fn describe_snapshots_filters_by_name() {
    let service = service_with_replication_group("filt-rg", 1);
    for name in &["snap-1", "snap-2"] {
        let req = request(
            "CreateSnapshot",
            &[("SnapshotName", name), ("ReplicationGroupId", "filt-rg")],
        );
        service.create_snapshot(&req).await.unwrap();
    }
    let req = request("DescribeSnapshots", &[("SnapshotName", "snap-1")]);
    let resp = service.describe_snapshots(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<SnapshotName>snap-1</SnapshotName>"));
    assert!(!body.contains("<SnapshotName>snap-2</SnapshotName>"));
}

#[tokio::test]
async fn describe_snapshots_filters_by_replication_group() {
    let service = service_with_replication_group("rg-a", 1);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "rg-a-snap"),
            ("ReplicationGroupId", "rg-a"),
        ],
    );
    service.create_snapshot(&req).await.unwrap();

    let req = request("DescribeSnapshots", &[("ReplicationGroupId", "rg-a")]);
    let resp = service.describe_snapshots(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<SnapshotName>rg-a-snap</SnapshotName>"));

    // Filter by non-matching group returns empty
    let req = request("DescribeSnapshots", &[("ReplicationGroupId", "rg-b")]);
    let resp = service.describe_snapshots(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(!body.contains("<SnapshotName>"));
}

#[test]
fn describe_snapshots_not_found_by_name() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    let req = request("DescribeSnapshots", &[("SnapshotName", "nope")]);
    assert!(service.describe_snapshots(&req).is_err());
}

#[tokio::test]
async fn delete_snapshot_removes_and_returns_deleting() {
    let service = service_with_replication_group("del-rg", 1);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "del-snap"),
            ("ReplicationGroupId", "del-rg"),
        ],
    );
    service.create_snapshot(&req).await.unwrap();

    let req = request("DeleteSnapshot", &[("SnapshotName", "del-snap")]);
    let resp = service.delete_snapshot(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<SnapshotStatus>deleting</SnapshotStatus>"));
    assert!(body.contains("<DeleteSnapshotResponse"));

    // Verify it's gone
    assert!(!service
        .state
        .read()
        .default_ref()
        .snapshots
        .contains_key("del-snap"));
}

#[tokio::test]
async fn delete_snapshot_cleans_up_tags() {
    let service = service_with_replication_group("tag-del-rg", 1);
    let req = request(
        "CreateSnapshot",
        &[
            ("SnapshotName", "tag-del-snap"),
            ("ReplicationGroupId", "tag-del-rg"),
        ],
    );
    service.create_snapshot(&req).await.unwrap();

    let arn = "arn:aws:elasticache:us-east-1:123456789012:snapshot:tag-del-snap".to_string();
    assert!(service.state.read().default_ref().tags.contains_key(&arn));

    let req = request("DeleteSnapshot", &[("SnapshotName", "tag-del-snap")]);
    service.delete_snapshot(&req).unwrap();
    assert!(!service.state.read().default_ref().tags.contains_key(&arn));
}

#[test]
fn delete_snapshot_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);
    let req = request("DeleteSnapshot", &[("SnapshotName", "nope")]);
    assert!(service.delete_snapshot(&req).is_err());
}

#[test]
fn snapshot_xml_contains_all_fields() {
    let snap = CacheSnapshot {
        snapshot_name: "test-snap".to_string(),
        replication_group_id: "rg-1".to_string(),
        replication_group_description: "desc".to_string(),
        snapshot_status: "available".to_string(),
        cache_node_type: "cache.t3.micro".to_string(),
        engine: "redis".to_string(),
        engine_version: "7.1".to_string(),
        num_cache_clusters: 2,
        arn: "arn:aws:elasticache:us-east-1:123:snapshot:test-snap".to_string(),
        created_at: "2024-01-01T00:00:00Z".to_string(),
        snapshot_source: "manual".to_string(),
        rdb_path: None,
    };
    let xml = snapshot_xml(&snap);
    assert!(xml.contains("<SnapshotName>test-snap</SnapshotName>"));
    assert!(xml.contains("<ReplicationGroupId>rg-1</ReplicationGroupId>"));
    assert!(xml.contains("<SnapshotStatus>available</SnapshotStatus>"));
    assert!(xml.contains("<SnapshotSource>manual</SnapshotSource>"));
    assert!(xml.contains("<CacheNodeType>cache.t3.micro</CacheNodeType>"));
    assert!(xml.contains("<Engine>redis</Engine>"));
    assert!(xml.contains("<EngineVersion>7.1</EngineVersion>"));
    assert!(xml.contains("<NumCacheClusters>2</NumCacheClusters>"));
    assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:snapshot:test-snap</ARN>"));
}

// ── Error branch tests ──

fn expect_ec_err(result: Result<AwsResponse, AwsServiceError>, code: &str) {
    match result {
        Err(e) => assert_eq!(e.code(), code, "wrong error code: {e}"),
        Ok(_) => panic!("expected error {code}, got Ok"),
    }
}

#[test]
fn describe_cache_cluster_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.describe_cache_clusters(&request(
            "DescribeCacheClusters",
            &[("CacheClusterId", "nope")],
        )),
        "CacheClusterNotFound",
    );
}

#[tokio::test]
async fn delete_cache_cluster_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.delete_cache_cluster(&request(
            "DeleteCacheClusters",
            &[("CacheClusterId", "nope")],
        ))
        .await,
        "CacheClusterNotFound",
    );
}

#[test]
fn describe_replication_group_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.describe_replication_groups(&request(
            "DescribeReplicationGroups",
            &[("ReplicationGroupId", "nope")],
        )),
        "ReplicationGroupNotFoundFault",
    );
}

#[tokio::test]
async fn delete_replication_group_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.delete_replication_group(&request(
            "DeleteReplicationGroup",
            &[("ReplicationGroupId", "nope")],
        ))
        .await,
        "ReplicationGroupNotFoundFault",
    );
}

#[test]
fn describe_serverless_cache_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.describe_serverless_caches(&request(
            "DescribeServerlessCaches",
            &[("ServerlessCacheName", "nope")],
        )),
        "ServerlessCacheNotFoundFault",
    );
}

#[tokio::test]
async fn delete_serverless_cache_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.delete_serverless_cache(&request(
            "DeleteServerlessCache",
            &[("ServerlessCacheName", "nope")],
        ))
        .await,
        "ServerlessCacheNotFoundFault",
    );
}

#[test]
fn describe_user_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.describe_users(&request("DescribeUsers", &[("UserId", "nope")])),
        "UserNotFound",
    );
}

#[test]
fn delete_user_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.delete_user(&request("DeleteUser", &[("UserId", "nope")])),
        "UserNotFound",
    );
}

#[test]
fn describe_user_group_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.describe_user_groups(&request("DescribeUserGroups", &[("UserGroupId", "nope")])),
        "UserGroupNotFound",
    );
}

#[test]
fn delete_user_group_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.delete_user_group(&request("DeleteUserGroup", &[("UserGroupId", "nope")])),
        "UserGroupNotFound",
    );
}

#[test]
fn describe_cache_subnet_group_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.describe_cache_subnet_groups(&request(
            "DescribeCacheSubnetGroups",
            &[("CacheSubnetGroupName", "nope")],
        )),
        "CacheSubnetGroupNotFoundFault",
    );
}

#[test]
fn delete_cache_subnet_group_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.delete_cache_subnet_group(&request(
            "DeleteCacheSubnetGroup",
            &[("CacheSubnetGroupName", "nope")],
        )),
        "CacheSubnetGroupNotFoundFault",
    );
}

#[test]
fn describe_snapshot_not_found() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.describe_snapshots(&request("DescribeSnapshots", &[("SnapshotName", "nope")])),
        "SnapshotNotFoundFault",
    );
}

#[test]
fn delete_snapshot_nonexistent() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    expect_ec_err(
        svc.delete_snapshot(&request("DeleteSnapshot", &[("SnapshotName", "nope")])),
        "SnapshotNotFoundFault",
    );
}

#[test]
fn create_user_duplicate() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request(
        "CreateUser",
        &[
            ("UserId", "dup"),
            ("UserName", "dup"),
            ("Engine", "redis"),
            ("AccessString", "on ~* +@all"),
        ],
    );
    svc.create_user(&req).unwrap();
    expect_ec_err(svc.create_user(&req), "UserAlreadyExists");
}

// ── Describe cache engine versions ──

#[test]
fn describe_cache_engine_versions() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);

    let resp = svc
        .describe_cache_engine_versions(&request("DescribeCacheEngineVersions", &[]))
        .unwrap();
    let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(xml.contains("CacheEngineVersions"));
}

// ── Reserved cache nodes offerings ──

#[test]
fn describe_reserved_cache_nodes_offerings_basic() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);

    let resp = svc
        .describe_reserved_cache_nodes_offerings(&request(
            "DescribeReservedCacheNodesOfferings",
            &[],
        ))
        .unwrap();
    let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(xml.contains("ReservedCacheNodesOfferings"));
}

#[test]
fn describe_reserved_cache_nodes_empty() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);

    let resp = svc
        .describe_reserved_cache_nodes(&request("DescribeReservedCacheNodes", &[]))
        .unwrap();
    let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(xml.contains("ReservedCacheNodes"));
}

// ── Subnet group lifecycle ──

#[test]
fn subnet_group_create_describe_delete() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);

    svc.create_cache_subnet_group(&request(
        "CreateCacheSubnetGroup",
        &[
            ("CacheSubnetGroupName", "my-sn"),
            ("CacheSubnetGroupDescription", "desc"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-123"),
        ],
    ))
    .unwrap();

    svc.describe_cache_subnet_groups(&request(
        "DescribeCacheSubnetGroups",
        &[("CacheSubnetGroupName", "my-sn")],
    ))
    .unwrap();

    svc.delete_cache_subnet_group(&request(
        "DeleteCacheSubnetGroup",
        &[("CacheSubnetGroupName", "my-sn")],
    ))
    .unwrap();
}

// ── Global replication group operations ──

#[test]
fn describe_global_replication_groups_empty() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);

    let resp = svc
        .describe_global_replication_groups(&request("DescribeGlobalReplicationGroups", &[]))
        .unwrap();
    let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(xml.contains("GlobalReplicationGroups"));
}

// ── user missing/invalid fields ──

#[test]
fn create_user_missing_user_id_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("CreateUser", &[("UserName", "u"), ("Engine", "redis")]);
    assert!(svc.create_user(&req).is_err());
}

#[test]
fn create_user_missing_engine_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("CreateUser", &[("UserId", "u1"), ("UserName", "u")]);
    assert!(svc.create_user(&req).is_err());
}

#[test]
fn delete_user_group_not_found_is_error() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DeleteUserGroup", &[("UserGroupId", "ghost")]);
    assert!(svc.delete_user_group(&req).is_err());
}

// ── cache cluster error paths ──

#[test]
fn describe_cache_clusters_invalid_marker_returns_error() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    // no clusters, marker for a nonexistent cluster - returns empty list
    let req = request("DescribeCacheClusters", &[]);
    let resp = svc.describe_cache_clusters(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("DescribeCacheClustersResult"));
}

#[tokio::test]
async fn delete_cache_cluster_missing_id_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DeleteCacheCluster", &[]);
    assert!(svc.delete_cache_cluster(&req).await.is_err());
}

#[test]
fn add_tags_missing_arn_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("AddTagsToResource", &[("Tags.Tag.1.Key", "k")]);
    assert!(svc.add_tags_to_resource(&req).is_err());
}

#[test]
fn list_tags_missing_arn_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("ListTagsForResource", &[]);
    assert!(svc.list_tags_for_resource(&req).is_err());
}

#[test]
fn remove_tags_missing_arn_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("RemoveTagsFromResource", &[("TagKeys.member.1", "k")]);
    assert!(svc.remove_tags_from_resource(&req).is_err());
}

// ── replication group error paths ──

#[tokio::test]
async fn create_replication_group_missing_id_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request(
        "CreateReplicationGroup",
        &[("ReplicationGroupDescription", "desc")],
    );
    assert!(svc.create_replication_group(&req).await.is_err());
}

#[tokio::test]
async fn create_replication_group_missing_description_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("CreateReplicationGroup", &[("ReplicationGroupId", "rg")]);
    assert!(svc.create_replication_group(&req).await.is_err());
}

#[tokio::test]
async fn create_replication_group_rejects_num_node_groups_out_of_range() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    // Default Redis 7.1 caps at 500; reject anything above.
    let req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "huge-rg"),
            ("ReplicationGroupDescription", "too many shards"),
            ("NumNodeGroups", "1000000"),
        ],
    );
    match svc.create_replication_group(&req).await {
        Ok(_) => panic!("expected NumNodeGroups=1000000 to be rejected"),
        Err(e) => assert_eq!(e.status(), http::StatusCode::BAD_REQUEST),
    }

    let req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "zero-rg"),
            ("ReplicationGroupDescription", "zero shards"),
            ("NumNodeGroups", "0"),
        ],
    );
    match svc.create_replication_group(&req).await {
        Ok(_) => panic!("expected NumNodeGroups=0 to be rejected"),
        Err(e) => assert_eq!(e.status(), http::StatusCode::BAD_REQUEST),
    }
}

#[test]
fn max_node_groups_matches_engine_version() {
    // Redis < 5.0.6 was capped at 90 shards. Reject the request the same
    // way real AWS would for callers pinning to those legacy versions.
    assert_eq!(super::max_node_groups_for("redis", "3.2.10"), 90);
    assert_eq!(super::max_node_groups_for("redis", "5.0.0"), 90);
    assert_eq!(super::max_node_groups_for("redis", "5.0.5"), 90);
    // 5.0.6 is the threshold where AWS lifted the cap.
    assert_eq!(super::max_node_groups_for("redis", "5.0.6"), 500);
    assert_eq!(super::max_node_groups_for("redis", "6.2"), 500);
    assert_eq!(super::max_node_groups_for("redis", "7.1"), 500);
    // Valkey is unaffected; cap is always 500.
    assert_eq!(super::max_node_groups_for("valkey", "7.0"), 500);
    assert_eq!(super::max_node_groups_for("valkey", "8.0"), 500);
    // Memcached doesn't have replication groups, but the helper is total.
    assert_eq!(super::max_node_groups_for("memcached", "1.6.22"), 500);
    // Unparseable versions fall back to the safe modern ceiling.
    assert_eq!(super::max_node_groups_for("redis", "garbage"), 500);
}

#[tokio::test]
async fn create_replication_group_rejects_num_node_groups_above_legacy_redis_cap() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    // 100 shards on Redis 5.0.5 must be rejected (90 ceiling), but allowed
    // on Redis 7.1 (500 ceiling) so we know the cap is engine-version-aware.
    let req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "legacy-rg"),
            ("ReplicationGroupDescription", "legacy redis"),
            ("EngineVersion", "5.0.5"),
            ("NumNodeGroups", "100"),
        ],
    );
    match svc.create_replication_group(&req).await {
        Ok(_) => panic!("Redis 5.0.5 should cap NumNodeGroups at 90"),
        Err(e) => {
            assert_eq!(e.status(), http::StatusCode::BAD_REQUEST);
            assert!(
                e.message().contains("90"),
                "error must mention engine-specific cap, got: {}",
                e.message()
            );
        }
    }
}

// Build a ReplicationGroup as if `CreateReplicationGroup` had run, by
// running the same parsing path the handler does (sans runtime). Used
// by the persistence round-trip tests below to exercise every input
// the handler claims to accept.
fn replication_group_from_request(req: &AwsRequest) -> crate::state::ReplicationGroup {
    let id = required_query_param(req, "ReplicationGroupId").unwrap();
    let description = required_query_param(req, "ReplicationGroupDescription").unwrap();
    let engine = optional_query_param(req, "Engine").unwrap_or_else(|| ENGINE_REDIS.to_string());
    let engine_version = optional_query_param(req, "EngineVersion").unwrap_or("7.1".into());
    let cache_node_type =
        optional_query_param(req, "CacheNodeType").unwrap_or("cache.t3.micro".into());
    let num_cache_clusters: i32 = optional_query_param(req, "NumCacheClusters")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let automatic_failover_enabled =
        parse_optional_bool(optional_query_param(req, "AutomaticFailoverEnabled").as_deref())
            .unwrap()
            .unwrap_or(false);
    let transit_encryption_enabled =
        parse_optional_bool(optional_query_param(req, "TransitEncryptionEnabled").as_deref())
            .unwrap()
            .unwrap_or(false);
    let at_rest_encryption_enabled =
        parse_optional_bool(optional_query_param(req, "AtRestEncryptionEnabled").as_deref())
            .unwrap()
            .unwrap_or(false);
    let multi_az_enabled =
        parse_optional_bool(optional_query_param(req, "MultiAZEnabled").as_deref())
            .unwrap()
            .unwrap_or(false);
    let auth_token = optional_query_param(req, "AuthToken");
    let auth_token_enabled = auth_token.is_some();
    let kms_key_id = optional_query_param(req, "KmsKeyId");
    let user_group_ids = parse_query_list_param(req, "UserGroupIds", "UserGroupId");
    let num_node_groups: i32 = optional_query_param(req, "NumNodeGroups")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let replicas_per_node_group: Option<i32> =
        optional_query_param(req, "ReplicasPerNodeGroup").and_then(|v| v.parse().ok());
    let data_tiering_enabled =
        parse_optional_bool(optional_query_param(req, "DataTieringEnabled").as_deref()).unwrap();
    let data_tiering =
        data_tiering_enabled.map(|b| if b { "enabled" } else { "disabled" }.to_string());
    let ip_discovery = optional_query_param(req, "IpDiscovery");
    let network_type =
        Some(optional_query_param(req, "NetworkType").unwrap_or_else(|| "ipv4".into()));
    let transit_encryption_mode = optional_query_param(req, "TransitEncryptionMode");
    let log_delivery_configurations = parse_log_delivery_configs(req);
    let notification_topic_arn = optional_query_param(req, "NotificationTopicArn");
    let cluster_mode = optional_query_param(req, "ClusterMode");
    let cluster_enabled = num_node_groups > 1
        || cluster_mode.as_deref() == Some("enabled")
        || cluster_mode.as_deref() == Some("compatible");
    let port: u16 = optional_query_param(req, "Port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(6379);
    let arn = format!("arn:aws:elasticache:us-east-1:123456789012:replicationgroup:{id}");
    crate::state::ReplicationGroup {
        replication_group_id: id.clone(),
        description,
        global_replication_group_id: None,
        global_replication_group_role: None,
        status: "available".to_string(),
        cache_node_type,
        engine,
        engine_version,
        num_cache_clusters,
        automatic_failover_enabled,
        endpoint_address: "127.0.0.1".to_string(),
        endpoint_port: port,
        arn,
        created_at: "2024-01-01T00:00:00Z".to_string(),
        container_id: "test-container".to_string(),
        host_port: port,
        member_clusters: (1..=num_cache_clusters)
            .map(|i| format!("{id}-{i:03}"))
            .collect(),
        snapshot_retention_limit: 0,
        snapshot_window: "05:00-09:00".to_string(),
        transit_encryption_enabled,
        at_rest_encryption_enabled,
        cluster_enabled,
        kms_key_id,
        auth_token_enabled,
        user_group_ids,
        multi_az_enabled,
        log_delivery_configurations,
        data_tiering,
        ip_discovery,
        network_type,
        transit_encryption_mode,
        num_node_groups,
        configuration_endpoint_address: cluster_enabled.then(|| "127.0.0.1".to_string()),
        configuration_endpoint_port: cluster_enabled.then_some(port),
        replicas_per_node_group,
        auth_token,
        port,
        notification_topic_arn,
        cluster_mode,
        data_tiering_enabled,
        notification_topic_status: None,
        cache_parameter_group_name: optional_query_param(req, "CacheParameterGroupName"),
        cache_subnet_group_name: optional_query_param(req, "CacheSubnetGroupName"),
        security_group_ids: parse_query_list_param(req, "SecurityGroupIds", "SecurityGroupId"),
        preferred_maintenance_window: optional_query_param(req, "PreferredMaintenanceWindow"),
        snapshot_name: optional_query_param(req, "SnapshotName"),
        snapshot_arns: parse_query_list_param(req, "SnapshotArns", "SnapshotArn"),
        auto_minor_version_upgrade: parse_optional_bool(
            optional_query_param(req, "AutoMinorVersionUpgrade").as_deref(),
        )
        .unwrap()
        .unwrap_or(true),
    }
}

fn service_with_replication_group_from_request(req: &AwsRequest) -> ElastiCacheService {
    let group = replication_group_from_request(req);
    let shared: SharedElastiCacheState = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut __a = shared.write();
        let s = __a.default_mut();
        s.tags.insert(group.arn.clone(), Vec::new());
        s.replication_groups
            .insert(group.replication_group_id.clone(), group);
    }
    ElastiCacheService::new(shared)
}

#[test]
fn create_replication_group_persists_all_encryption_fields() {
    let create_req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "enc-rg"),
            ("ReplicationGroupDescription", "encryption fields"),
            ("AuthToken", "supersecret"),
            ("TransitEncryptionEnabled", "true"),
            ("AtRestEncryptionEnabled", "true"),
            ("KmsKeyId", "arn:aws:kms:us-east-1:123:key/abc-123"),
        ],
    );
    let svc = service_with_replication_group_from_request(&create_req);

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "enc-rg")],
    );
    let resp = svc.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<TransitEncryptionEnabled>true</TransitEncryptionEnabled>"));
    assert!(body.contains("<AtRestEncryptionEnabled>true</AtRestEncryptionEnabled>"));
    assert!(body.contains("<AuthTokenEnabled>true</AuthTokenEnabled>"));
    assert!(body.contains("<KmsKeyId>arn:aws:kms:us-east-1:123:key/abc-123</KmsKeyId>"));
    // Auth token itself must never be echoed back.
    assert!(!body.contains("supersecret"));
}

#[test]
fn create_replication_group_persists_log_delivery_configurations() {
    let create_req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "log-rg"),
            ("ReplicationGroupDescription", "log delivery"),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogType",
                "slow-log",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationType",
                "cloudwatch-logs",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationDetails.CloudWatchLogsDetails.LogGroup",
                "/aws/elasticache/slow",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogFormat",
                "json",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.LogType",
                "engine-log",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.DestinationType",
                "kinesis-firehose",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.DestinationDetails.KinesisFirehoseDetails.DeliveryStream",
                "engine-stream",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.LogFormat",
                "text",
            ),
        ],
    );
    let svc = service_with_replication_group_from_request(&create_req);

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "log-rg")],
    );
    let resp = svc.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<LogType>slow-log</LogType>"));
    assert!(body.contains("<DestinationType>cloudwatch-logs</DestinationType>"));
    assert!(body.contains("<LogGroup>/aws/elasticache/slow</LogGroup>"));
    assert!(body.contains("<LogType>engine-log</LogType>"));
    assert!(body.contains("<DestinationType>kinesis-firehose</DestinationType>"));
    assert!(body.contains("<DeliveryStream>engine-stream</DeliveryStream>"));
    assert!(body.contains("<LogFormat>text</LogFormat>"));
}

#[test]
fn create_replication_group_persists_network_fields() {
    let create_req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "net-rg"),
            ("ReplicationGroupDescription", "network fields"),
            ("IpDiscovery", "ipv6"),
            ("NetworkType", "dual_stack"),
            ("Port", "6380"),
        ],
    );
    let svc = service_with_replication_group_from_request(&create_req);

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "net-rg")],
    );
    let resp = svc.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<IpDiscovery>ipv6</IpDiscovery>"));
    assert!(body.contains("<NetworkType>dual_stack</NetworkType>"));
    assert!(body.contains("<Port>6380</Port>"));
}

#[test]
fn create_replication_group_persists_cluster_mode_and_user_groups() {
    let create_req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "cm-rg"),
            ("ReplicationGroupDescription", "cluster mode + user groups"),
            ("ClusterMode", "enabled"),
            ("UserGroupIds.UserGroupId.1", "ug-prod"),
            ("UserGroupIds.UserGroupId.2", "ug-readonly"),
        ],
    );
    let svc = service_with_replication_group_from_request(&create_req);

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "cm-rg")],
    );
    let resp = svc.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ClusterMode>enabled</ClusterMode>"));
    assert!(body.contains("<member>ug-prod</member>"));
    assert!(body.contains("<member>ug-readonly</member>"));
    // ClusterMode=enabled must imply ClusterEnabled=true downstream.
    assert!(body.contains("<ClusterEnabled>true</ClusterEnabled>"));
}

#[test]
fn create_replication_group_defaults_port_and_network_type() {
    let create_req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "default-rg"),
            ("ReplicationGroupDescription", "defaults"),
        ],
    );
    let svc = service_with_replication_group_from_request(&create_req);

    let describe = request(
        "DescribeReplicationGroups",
        &[("ReplicationGroupId", "default-rg")],
    );
    let resp = svc.describe_replication_groups(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Port>6379</Port>"));
    assert!(body.contains("<NetworkType>ipv4</NetworkType>"));
}

// ── cache subnet group ──

#[test]
fn create_cache_subnet_group_missing_id_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request(
        "CreateCacheSubnetGroup",
        &[
            ("CacheSubnetGroupDescription", "d"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-a"),
        ],
    );
    assert!(svc.create_cache_subnet_group(&req).is_err());
}

#[test]
fn create_cache_subnet_group_duplicate_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let params = &[
        ("CacheSubnetGroupName", "sg"),
        ("CacheSubnetGroupDescription", "d"),
        ("SubnetIds.SubnetIdentifier.1", "subnet-a"),
    ];
    let req = request("CreateCacheSubnetGroup", params);
    svc.create_cache_subnet_group(&req).unwrap();
    let req = request("CreateCacheSubnetGroup", params);
    assert!(svc.create_cache_subnet_group(&req).is_err());
}

// ── snapshot error paths ──

#[test]
fn describe_snapshots_empty_returns_ok() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DescribeSnapshots", &[]);
    let resp = svc.describe_snapshots(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("DescribeSnapshotsResult"));
}

// ── serverless cache ──

#[tokio::test]
async fn create_serverless_cache_missing_name_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("CreateServerlessCache", &[("Engine", "redis")]);
    assert!(svc.create_serverless_cache(&req).await.is_err());
}

#[tokio::test]
async fn delete_serverless_cache_missing_name_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DeleteServerlessCache", &[]);
    assert!(svc.delete_serverless_cache(&req).await.is_err());
}

// ── global replication group missing fields ──

#[test]
fn create_global_replication_group_missing_id_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("CreateGlobalReplicationGroup", &[]);
    assert!(svc.create_global_replication_group(&req).is_err());
}

#[test]
fn describe_replication_groups_empty_ok() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DescribeReplicationGroups", &[]);
    let resp = svc.describe_replication_groups(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("DescribeReplicationGroupsResult"));
}

#[test]
fn describe_cache_parameter_groups_has_defaults() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DescribeCacheParameterGroups", &[]);
    let resp = svc.describe_cache_parameter_groups(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("CacheParameterGroup"));
}

#[test]
fn describe_cache_subnet_groups_has_defaults() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DescribeCacheSubnetGroups", &[]);
    let resp = svc.describe_cache_subnet_groups(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("DescribeCacheSubnetGroupsResult"));
}

#[test]
fn describe_user_groups_empty_ok() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DescribeUserGroups", &[]);
    let resp = svc.describe_user_groups(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("DescribeUserGroupsResult"));
}

#[tokio::test]
async fn create_cache_cluster_unsupported_engine_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "c1"),
            ("CacheNodeType", "cache.t3.micro"),
            ("Engine", "unknown-engine"),
            ("NumCacheNodes", "1"),
        ],
    );
    assert!(svc.create_cache_cluster(&req).await.is_err());
}

#[tokio::test]
async fn delete_cache_cluster_unknown_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DeleteCacheCluster", &[("CacheClusterId", "ghost")]);
    assert!(svc.delete_cache_cluster(&req).await.is_err());
}

#[test]
fn delete_user_unknown_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request("DeleteUser", &[("UserId", "ghost")]);
    assert!(svc.delete_user(&req).is_err());
}

#[test]
fn list_tags_unknown_arn_errors() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request(
        "ListTagsForResource",
        &[(
            "ResourceName",
            "arn:aws:elasticache:us-east-1:123:cluster/ghost",
        )],
    );
    assert!(svc.list_tags_for_resource(&req).is_err());
}

// ── Coverage for the closure batch ──

fn fresh_service() -> ElastiCacheService {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    ElastiCacheService::new(shared)
}

fn body(resp: AwsResponse) -> String {
    String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
}

#[test]
fn cache_security_group_lifecycle_unit() {
    let svc = fresh_service();
    let create = request(
        "CreateCacheSecurityGroup",
        &[("CacheSecurityGroupName", "sg1"), ("Description", "d")],
    );
    let resp = svc.create_cache_security_group(&create).unwrap();
    assert!(body(resp).contains("sg1"));

    let auth = request(
        "AuthorizeCacheSecurityGroupIngress",
        &[
            ("CacheSecurityGroupName", "sg1"),
            ("EC2SecurityGroupName", "ec2"),
            ("EC2SecurityGroupOwnerId", "111122223333"),
        ],
    );
    svc.authorize_cache_security_group_ingress(&auth).unwrap();

    let dup_auth = request(
        "AuthorizeCacheSecurityGroupIngress",
        &[
            ("CacheSecurityGroupName", "sg1"),
            ("EC2SecurityGroupName", "ec2"),
            ("EC2SecurityGroupOwnerId", "111122223333"),
        ],
    );
    let err = match svc.authorize_cache_security_group_ingress(&dup_auth) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "AuthorizationAlreadyExists");

    let revoke = request(
        "RevokeCacheSecurityGroupIngress",
        &[
            ("CacheSecurityGroupName", "sg1"),
            ("EC2SecurityGroupName", "ec2"),
            ("EC2SecurityGroupOwnerId", "111122223333"),
        ],
    );
    svc.revoke_cache_security_group_ingress(&revoke).unwrap();

    let revoke_unknown = request(
        "RevokeCacheSecurityGroupIngress",
        &[
            ("CacheSecurityGroupName", "sg1"),
            ("EC2SecurityGroupName", "no-such"),
            ("EC2SecurityGroupOwnerId", "111122223333"),
        ],
    );
    let err = match svc.revoke_cache_security_group_ingress(&revoke_unknown) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "AuthorizationNotFound");

    let list = request("DescribeCacheSecurityGroups", &[]);
    let resp = svc.describe_cache_security_groups(&list).unwrap();
    assert!(body(resp).contains("sg1"));

    let del = request(
        "DeleteCacheSecurityGroup",
        &[("CacheSecurityGroupName", "sg1")],
    );
    svc.delete_cache_security_group(&del).unwrap();
}

#[test]
fn cache_security_group_create_duplicate_errors() {
    let svc = fresh_service();
    let create = request(
        "CreateCacheSecurityGroup",
        &[("CacheSecurityGroupName", "sg2"), ("Description", "d")],
    );
    svc.create_cache_security_group(&create).unwrap();
    let err = match svc.create_cache_security_group(&create) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "CacheSecurityGroupAlreadyExists");
}

#[test]
fn delete_unknown_security_group_errors() {
    let svc = fresh_service();
    let req = request(
        "DeleteCacheSecurityGroup",
        &[("CacheSecurityGroupName", "ghost")],
    );
    let err = match svc.delete_cache_security_group(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "CacheSecurityGroupNotFound");
}

#[tokio::test]
async fn cache_parameter_group_full_lifecycle_unit() {
    let svc = fresh_service();
    let create = request(
        "CreateCacheParameterGroup",
        &[
            ("CacheParameterGroupName", "pg1"),
            ("CacheParameterGroupFamily", "redis7"),
            ("Description", "test"),
        ],
    );
    svc.create_cache_parameter_group(&create).unwrap();

    let modify = request(
        "ModifyCacheParameterGroup",
        &[
            ("CacheParameterGroupName", "pg1"),
            (
                "ParameterNameValues.member.1.ParameterName",
                "maxmemory-policy",
            ),
            ("ParameterNameValues.member.1.ParameterValue", "allkeys-lru"),
        ],
    );
    svc.modify_cache_parameter_group(&modify).await.unwrap();

    let describe = request(
        "DescribeCacheParameters",
        &[("CacheParameterGroupName", "pg1")],
    );
    let resp = svc.describe_cache_parameters(&describe).unwrap();
    assert!(body(resp).contains("maxmemory-policy"));

    let reset = request(
        "ResetCacheParameterGroup",
        &[
            ("CacheParameterGroupName", "pg1"),
            ("ResetAllParameters", "true"),
        ],
    );
    svc.reset_cache_parameter_group(&reset).unwrap();

    let del = request(
        "DeleteCacheParameterGroup",
        &[("CacheParameterGroupName", "pg1")],
    );
    svc.delete_cache_parameter_group(&del).unwrap();
}

#[test]
fn create_parameter_group_duplicate_errors() {
    let svc = fresh_service();
    let create = request(
        "CreateCacheParameterGroup",
        &[
            ("CacheParameterGroupName", "pg2"),
            ("CacheParameterGroupFamily", "redis7"),
            ("Description", "test"),
        ],
    );
    svc.create_cache_parameter_group(&create).unwrap();
    let err = match svc.create_cache_parameter_group(&create) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "CacheParameterGroupAlreadyExists");
}

#[test]
fn describe_cache_parameters_unknown_group_errors() {
    let svc = fresh_service();
    let req = request(
        "DescribeCacheParameters",
        &[("CacheParameterGroupName", "ghost")],
    );
    let err = match svc.describe_cache_parameters(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "CacheParameterGroupNotFound");
}

#[test]
fn list_allowed_node_type_modifications_returns_lists() {
    let svc = fresh_service();
    let req = request("ListAllowedNodeTypeModifications", &[]);
    let resp = svc.list_allowed_node_type_modifications(&req).unwrap();
    let b = body(resp);
    assert!(b.contains("ScaleUpModifications"));
    assert!(b.contains("cache.t4g.medium"));
}

#[test]
fn list_origination_numbers_seeds_default() {
    // Sanity check on the parameter-group default seed by hitting an
    // unrelated read endpoint that should always succeed.
    let svc = fresh_service();
    let req = request("DescribeCacheParameterGroups", &[]);
    let resp = svc.describe_cache_parameter_groups(&req).unwrap();
    assert!(body(resp).contains("CacheParameterGroups"));
}

#[test]
fn modify_unknown_cache_cluster_errors() {
    let svc = fresh_service();
    let req = request(
        "ModifyCacheCluster",
        &[("CacheClusterId", "ghost"), ("NumCacheNodes", "2")],
    );
    let err = match svc.modify_cache_cluster(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "CacheClusterNotFound");
}

#[tokio::test]
async fn reboot_unknown_cluster_errors() {
    let svc = fresh_service();
    let req = request("RebootCacheCluster", &[("CacheClusterId", "ghost")]);
    let err = match svc.reboot_cache_cluster(&req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "CacheClusterNotFound");
}

#[tokio::test]
async fn reboot_cache_cluster_marks_rebooting_when_no_runtime() {
    // Without a wired-up container runtime the API still flips status
    // to "rebooting cache cluster nodes" so SDK clients can observe
    // the reboot transition.
    let svc = fresh_service();
    {
        let mut state = svc.state.write();
        let account = state.get_or_create("123456789012");
        account.cache_clusters.insert(
            "test-cc".to_string(),
            CacheCluster {
                cache_cluster_id: "test-cc".to_string(),
                cache_node_type: "cache.t4g.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                cache_cluster_status: "available".to_string(),
                num_cache_nodes: 1,
                preferred_availability_zone: "us-east-1a".to_string(),
                cache_subnet_group_name: None,
                auto_minor_version_upgrade: true,
                arn: "arn:aws:elasticache:us-east-1:123456789012:cluster:test-cc".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6379,
                container_id: "abc123".to_string(),
                host_port: 6379,
                replication_group_id: None,
                cache_parameter_group_name: None,
                security_group_ids: Vec::new(),
                log_delivery_configurations: Vec::new(),
                transit_encryption_enabled: false,
                at_rest_encryption_enabled: false,
                auth_token_enabled: false,
                port: 6379,
                preferred_maintenance_window: None,
                preferred_availability_zones: Vec::new(),
                notification_topic_arn: None,
                cache_security_group_names: Vec::new(),
                snapshot_arns: Vec::new(),
                snapshot_name: None,
                snapshot_retention_limit: 0,
                snapshot_window: None,
                outpost_mode: None,
                preferred_outpost_arn: None,
                network_type: None,
                ip_discovery: None,
                az_mode: None,
                auth_token: None,
                kms_key_id: None,
                transit_encryption_mode: None,
                data_tiering_enabled: None,
                cluster_mode: None,
                preferred_outpost_arns: Vec::new(),
            },
        );
    }
    let req = request("RebootCacheCluster", &[("CacheClusterId", "test-cc")]);
    let resp = svc.reboot_cache_cluster(&req).await.unwrap();
    let body_str = body(resp);
    assert!(body_str.contains("rebooting cache cluster nodes"));
    let state = svc.state.read();
    let cluster = state
        .get("123456789012")
        .unwrap()
        .cache_clusters
        .get("test-cc")
        .unwrap();
    // Without a runtime, status remains in the rebooting transition
    // since fakecloud has nothing to verify the engine came back up.
    assert_eq!(
        cluster.cache_cluster_status,
        "rebooting cache cluster nodes"
    );
}

#[tokio::test]
async fn modify_unknown_user_errors() {
    let svc = fresh_service();
    let req = request("ModifyUser", &[("UserId", "ghost")]);
    let err = match svc.modify_user(&req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "UserNotFound");
}

#[tokio::test]
async fn modify_unknown_user_group_errors() {
    let svc = fresh_service();
    let req = request("ModifyUserGroup", &[("UserGroupId", "ghost")]);
    let err = match svc.modify_user_group(&req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "UserGroupNotFound");
}

#[test]
fn purchase_offering_unknown_id_errors() {
    let svc = fresh_service();
    let req = request(
        "PurchaseReservedCacheNodesOffering",
        &[("ReservedCacheNodesOfferingId", "no-such")],
    );
    let err = match svc.purchase_reserved_cache_nodes_offering(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "ReservedCacheNodesOfferingNotFound");
}

#[test]
fn describe_events_returns_empty() {
    let svc = fresh_service();
    let req = request("DescribeEvents", &[]);
    let resp = svc.describe_events(&req).unwrap();
    let b = body(resp);
    assert!(b.contains("<Events>"));
}

#[test]
fn describe_service_updates_returns_seeded_set() {
    let svc = fresh_service();
    let req = request("DescribeServiceUpdates", &[]);
    let resp = svc.describe_service_updates(&req).unwrap();
    let b = body(resp);
    assert!(b.contains("<ServiceUpdates>"));
    // Seeded, realistic service updates are returned (not an empty stub).
    assert!(
        b.contains("<ServiceUpdateName>elasticache-20240301-redis-security</ServiceUpdateName>")
    );
    assert!(b.contains("<ServiceUpdateStatus>available</ServiceUpdateStatus>"));
}

#[test]
fn describe_service_updates_filters_by_name() {
    let svc = fresh_service();
    let req = request(
        "DescribeServiceUpdates",
        &[(
            "ServiceUpdateName",
            "elasticache-20240601-memcached-security",
        )],
    );
    let b = body(svc.describe_service_updates(&req).unwrap());
    assert!(b.contains("elasticache-20240601-memcached-security"));
    assert!(!b.contains("elasticache-20240301-redis-security"));
}

fn insert_test_cluster(svc: &ElastiCacheService, id: &str) {
    let mut state = svc.state.write();
    let account = state.get_or_create("123456789012");
    account.cache_clusters.insert(
        id.to_string(),
        CacheCluster {
            cache_cluster_id: id.to_string(),
            cache_node_type: "cache.t4g.micro".to_string(),
            engine: "redis".to_string(),
            engine_version: "7.1".to_string(),
            cache_cluster_status: "available".to_string(),
            num_cache_nodes: 1,
            preferred_availability_zone: "us-east-1a".to_string(),
            cache_subnet_group_name: None,
            auto_minor_version_upgrade: true,
            arn: format!("arn:aws:elasticache:us-east-1:123456789012:cluster:{id}"),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: 6379,
            container_id: "abc123".to_string(),
            host_port: 6379,
            replication_group_id: None,
            cache_parameter_group_name: None,
            security_group_ids: Vec::new(),
            log_delivery_configurations: Vec::new(),
            transit_encryption_enabled: false,
            at_rest_encryption_enabled: false,
            auth_token_enabled: false,
            port: 6379,
            preferred_maintenance_window: None,
            preferred_availability_zones: Vec::new(),
            notification_topic_arn: None,
            cache_security_group_names: Vec::new(),
            snapshot_arns: Vec::new(),
            snapshot_name: None,
            snapshot_retention_limit: 0,
            snapshot_window: None,
            outpost_mode: None,
            preferred_outpost_arn: None,
            network_type: None,
            ip_discovery: None,
            az_mode: None,
            auth_token: None,
            kms_key_id: None,
            transit_encryption_mode: None,
            data_tiering_enabled: None,
            cluster_mode: None,
            preferred_outpost_arns: Vec::new(),
        },
    );
}

#[test]
fn describe_update_actions_materializes_for_clusters() {
    let svc = fresh_service();
    insert_test_cluster(&svc, "cc-1");

    let req = request("DescribeUpdateActions", &[]);
    let b = body(svc.describe_update_actions(&req).unwrap());
    // An action exists for the caller's cluster, initially not-applied.
    assert!(b.contains("<CacheClusterId>cc-1</CacheClusterId>"));
    assert!(b.contains("<UpdateActionStatus>not-applied</UpdateActionStatus>"));

    // Filtering by an unrelated cluster id returns none.
    let req = request(
        "DescribeUpdateActions",
        &[("CacheClusterIds.member.1", "other")],
    );
    let b = body(svc.describe_update_actions(&req).unwrap());
    assert!(!b.contains("<CacheClusterId>cc-1</CacheClusterId>"));
}

#[test]
fn batch_apply_update_action_transitions_state() {
    let svc = fresh_service();
    insert_test_cluster(&svc, "cc-1");
    let su = "elasticache-20240301-redis-security";

    // Apply the update action for the cluster.
    let req = request(
        "BatchApplyUpdateAction",
        &[
            ("ServiceUpdateName", su),
            ("CacheClusterIds.member.1", "cc-1"),
        ],
    );
    let b = body(svc.batch_apply_update_action(&req).unwrap());
    assert!(b.contains("<ProcessedUpdateActions>"));
    assert!(b.contains("<CacheClusterId>cc-1</CacheClusterId>"));
    assert!(b.contains("<UpdateActionStatus>waiting-to-start</UpdateActionStatus>"));

    // The transition persisted: DescribeUpdateActions scoped to this service
    // update now shows the new status (the other seeded update is untouched).
    let b = body(
        svc.describe_update_actions(&request(
            "DescribeUpdateActions",
            &[("ServiceUpdateName", su)],
        ))
        .unwrap(),
    );
    assert!(b.contains("<UpdateActionStatus>waiting-to-start</UpdateActionStatus>"));
    assert!(!b.contains("<UpdateActionStatus>not-applied</UpdateActionStatus>"));

    // An unknown cluster is reported as unprocessed, not fake-success.
    let req = request(
        "BatchApplyUpdateAction",
        &[
            ("ServiceUpdateName", su),
            ("CacheClusterIds.member.1", "ghost"),
        ],
    );
    let b = body(svc.batch_apply_update_action(&req).unwrap());
    assert!(b.contains("<UnprocessedUpdateActions>"));
    assert!(b.contains("<CacheClusterId>ghost</CacheClusterId>"));
    assert!(b.contains("UpdateActionNotFoundFault"));
}

#[test]
fn describe_update_actions_filters_by_engine_and_service_update_status() {
    let svc = fresh_service();
    insert_test_cluster(&svc, "cc-1");

    // Engine filter -> only the memcached service update's action.
    let b = body(
        svc.describe_update_actions(&request(
            "DescribeUpdateActions",
            &[("Engine", "memcached")],
        ))
        .unwrap(),
    );
    assert!(b.contains("elasticache-20240601-memcached-security"));
    assert!(!b.contains("elasticache-20240301-redis-security"));

    // ServiceUpdateStatus=available matches both seeded updates.
    let b = body(
        svc.describe_update_actions(&request(
            "DescribeUpdateActions",
            &[("ServiceUpdateStatus.member.1", "available")],
        ))
        .unwrap(),
    );
    assert!(b.contains("elasticache-20240601-memcached-security"));
    assert!(b.contains("elasticache-20240301-redis-security"));

    // A non-matching service-update status filters everything out.
    let b = body(
        svc.describe_update_actions(&request(
            "DescribeUpdateActions",
            &[("ServiceUpdateStatus.member.1", "expired")],
        ))
        .unwrap(),
    );
    assert!(!b.contains("<CacheClusterId>cc-1</CacheClusterId>"));
}

#[test]
fn update_actions_pruned_when_target_deleted() {
    let svc = fresh_service();
    insert_test_cluster(&svc, "cc-1");

    // Materialize the actions, then delete the target cluster from state.
    let b = body(
        svc.describe_update_actions(&request("DescribeUpdateActions", &[]))
            .unwrap(),
    );
    assert!(b.contains("<CacheClusterId>cc-1</CacheClusterId>"));
    {
        let mut state = svc.state.write();
        let account = state.get_or_create("123456789012");
        account.cache_clusters.remove("cc-1");
    }

    // Describe now reconciles: no action for the deleted cluster remains.
    let b = body(
        svc.describe_update_actions(&request("DescribeUpdateActions", &[]))
            .unwrap(),
    );
    assert!(!b.contains("<CacheClusterId>cc-1</CacheClusterId>"));

    // BatchApply against the deleted target reports it unprocessed, not success.
    let b = body(
        svc.batch_apply_update_action(&request(
            "BatchApplyUpdateAction",
            &[
                ("ServiceUpdateName", "elasticache-20240301-redis-security"),
                ("CacheClusterIds.member.1", "cc-1"),
            ],
        ))
        .unwrap(),
    );
    assert!(b.contains("<UnprocessedUpdateActions>"));
    assert!(b.contains("UpdateActionNotFoundFault"));
}

#[test]
fn copy_snapshot_unknown_source_errors() {
    let svc = fresh_service();
    let req = request(
        "CopySnapshot",
        &[
            ("SourceSnapshotName", "ghost"),
            ("TargetSnapshotName", "ghost-copy"),
        ],
    );
    let err = match svc.copy_snapshot(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "SnapshotNotFoundFault");
}

#[test]
fn copy_serverless_snapshot_unknown_source_errors() {
    let svc = fresh_service();
    let req = request(
        "CopyServerlessCacheSnapshot",
        &[
            ("SourceServerlessCacheSnapshotName", "ghost"),
            ("TargetServerlessCacheSnapshotName", "ghost-copy"),
        ],
    );
    let err = match svc.copy_serverless_cache_snapshot(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "ServerlessCacheSnapshotNotFoundFault");
}

#[test]
fn migration_ops_unknown_replication_group_errors() {
    let svc = fresh_service();
    let req = request("StartMigration", &[("ReplicationGroupId", "ghost")]);
    let err = match svc.start_migration(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "ReplicationGroupNotFoundFault");
}

// ── cache cluster create field round-trip (N2) ──
//
// Build a CacheCluster as if `CreateCacheCluster` had run, by re-running
// the same parser the handler uses (sans runtime). The runtime path needs
// docker, so unit tests skip it by inserting the fully-built struct directly.
fn cache_cluster_from_request(req: &AwsRequest) -> crate::state::CacheCluster {
    let cache_cluster_id = required_query_param(req, "CacheClusterId").unwrap();
    let engine = optional_query_param(req, "Engine").unwrap_or_else(|| ENGINE_REDIS.to_string());
    let default_version = match engine.as_str() {
        "valkey" => "8.0",
        "memcached" => "1.6.22",
        _ => "7.1",
    };
    let engine_version =
        optional_query_param(req, "EngineVersion").unwrap_or_else(|| default_version.to_string());
    let cache_node_type =
        optional_query_param(req, "CacheNodeType").unwrap_or_else(|| "cache.t3.micro".to_string());
    let num_cache_nodes: i32 = optional_query_param(req, "NumCacheNodes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let cache_subnet_group_name =
        optional_query_param(req, "CacheSubnetGroupName").or_else(|| Some("default".to_string()));
    let auto_minor_version_upgrade =
        parse_optional_bool(optional_query_param(req, "AutoMinorVersionUpgrade").as_deref())
            .unwrap()
            .unwrap_or(true);
    let cache_parameter_group_name = optional_query_param(req, "CacheParameterGroupName");
    let security_group_ids = parse_query_list_param(req, "SecurityGroupIds", "SecurityGroupId");
    let cache_security_group_names =
        parse_query_list_param(req, "CacheSecurityGroupNames", "CacheSecurityGroupName");
    let log_delivery_configurations = parse_log_delivery_configs(req);
    let transit_encryption_enabled =
        parse_optional_bool(optional_query_param(req, "TransitEncryptionEnabled").as_deref())
            .unwrap()
            .unwrap_or(false);
    let at_rest_encryption_enabled =
        parse_optional_bool(optional_query_param(req, "AtRestEncryptionEnabled").as_deref())
            .unwrap()
            .unwrap_or(false);
    let auth_token = optional_query_param(req, "AuthToken");
    let auth_token_enabled = auth_token.is_some();
    let default_port = if engine == "memcached" { 11211 } else { 6379 };
    let port: u16 = optional_query_param(req, "Port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(default_port);
    let preferred_availability_zone = optional_query_param(req, "PreferredAvailabilityZone")
        .unwrap_or_else(|| "us-east-1a".to_string());
    let preferred_availability_zones =
        parse_query_list_param(req, "PreferredAvailabilityZones", "AvailabilityZone");
    let preferred_maintenance_window = optional_query_param(req, "PreferredMaintenanceWindow");
    let notification_topic_arn = optional_query_param(req, "NotificationTopicArn");
    let snapshot_arns = parse_query_list_param(req, "SnapshotArns", "SnapshotArn");
    let snapshot_name = optional_query_param(req, "SnapshotName");
    let snapshot_retention_limit = optional_query_param(req, "SnapshotRetentionLimit")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let snapshot_window = optional_query_param(req, "SnapshotWindow");
    let outpost_mode = optional_query_param(req, "OutpostMode");
    let preferred_outpost_arn = optional_query_param(req, "PreferredOutpostArn");
    let network_type =
        Some(optional_query_param(req, "NetworkType").unwrap_or_else(|| "ipv4".into()));
    let ip_discovery =
        Some(optional_query_param(req, "IpDiscovery").unwrap_or_else(|| "ipv4".into()));
    let az_mode = Some(optional_query_param(req, "AZMode").unwrap_or_else(|| "single-az".into()));
    let kms_key_id = optional_query_param(req, "KmsKeyId");
    let transit_encryption_mode = optional_query_param(req, "TransitEncryptionMode");
    let data_tiering_enabled =
        parse_optional_bool(optional_query_param(req, "DataTieringEnabled").as_deref()).unwrap();
    let cluster_mode = optional_query_param(req, "ClusterMode");
    let preferred_outpost_arns =
        parse_query_list_param(req, "PreferredOutpostArns", "PreferredOutpostArn");
    let arn = format!("arn:aws:elasticache:us-east-1:123456789012:cluster:{cache_cluster_id}");
    crate::state::CacheCluster {
        cache_cluster_id: cache_cluster_id.clone(),
        cache_node_type,
        engine,
        engine_version,
        cache_cluster_status: "available".to_string(),
        num_cache_nodes,
        preferred_availability_zone,
        cache_subnet_group_name,
        auto_minor_version_upgrade,
        arn,
        created_at: "2024-01-01T00:00:00Z".to_string(),
        endpoint_address: "127.0.0.1".to_string(),
        endpoint_port: port,
        container_id: "test-container".to_string(),
        host_port: port,
        replication_group_id: None,
        cache_parameter_group_name,
        security_group_ids,
        log_delivery_configurations,
        transit_encryption_enabled,
        at_rest_encryption_enabled,
        auth_token_enabled,
        port,
        preferred_maintenance_window,
        preferred_availability_zones,
        notification_topic_arn,
        cache_security_group_names,
        snapshot_arns,
        snapshot_name,
        snapshot_retention_limit,
        snapshot_window,
        outpost_mode,
        preferred_outpost_arn,
        network_type,
        ip_discovery,
        az_mode,
        auth_token,
        kms_key_id,
        transit_encryption_mode,
        data_tiering_enabled,
        cluster_mode,
        preferred_outpost_arns,
    }
}

fn service_with_cache_cluster_from_request(req: &AwsRequest) -> ElastiCacheService {
    let cluster = cache_cluster_from_request(req);
    let shared: SharedElastiCacheState = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut __a = shared.write();
        let s = __a.default_mut();
        s.tags.insert(cluster.arn.clone(), Vec::new());
        s.cache_clusters
            .insert(cluster.cache_cluster_id.clone(), cluster);
    }
    ElastiCacheService::new(shared)
}

#[test]
fn create_cache_cluster_persists_log_delivery_configurations() {
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "log-cc"),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogType",
                "slow-log",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationType",
                "cloudwatch-logs",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationDetails.CloudWatchLogsDetails.LogGroup",
                "/aws/elasticache/slow",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogFormat",
                "json",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.LogType",
                "engine-log",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.DestinationType",
                "kinesis-firehose",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.DestinationDetails.KinesisFirehoseDetails.DeliveryStream",
                "engine-stream",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.2.LogFormat",
                "text",
            ),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let describe = request("DescribeCacheClusters", &[("CacheClusterId", "log-cc")]);
    let resp = svc.describe_cache_clusters(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<LogType>slow-log</LogType>"));
    assert!(body.contains("<DestinationType>cloudwatch-logs</DestinationType>"));
    assert!(body.contains("<LogGroup>/aws/elasticache/slow</LogGroup>"));
    assert!(body.contains("<LogType>engine-log</LogType>"));
    assert!(body.contains("<DestinationType>kinesis-firehose</DestinationType>"));
    assert!(body.contains("<DeliveryStream>engine-stream</DeliveryStream>"));
    assert!(body.contains("<LogFormat>text</LogFormat>"));
}

#[test]
fn create_cache_cluster_persists_security_and_subnet_groups() {
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "sec-cc"),
            ("CacheSubnetGroupName", "default"),
            ("SecurityGroupIds.SecurityGroupId.1", "sg-aaa"),
            ("SecurityGroupIds.SecurityGroupId.2", "sg-bbb"),
            ("CacheSecurityGroupNames.CacheSecurityGroupName.1", "ec2c-1"),
            ("CacheSecurityGroupNames.CacheSecurityGroupName.2", "ec2c-2"),
            ("CacheParameterGroupName", "default.redis7"),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let describe = request("DescribeCacheClusters", &[("CacheClusterId", "sec-cc")]);
    let resp = svc.describe_cache_clusters(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheSubnetGroupName>default</CacheSubnetGroupName>"));
    assert!(body.contains("<SecurityGroupId>sg-aaa</SecurityGroupId>"));
    assert!(body.contains("<SecurityGroupId>sg-bbb</SecurityGroupId>"));
    assert!(body.contains("<CacheSecurityGroupName>ec2c-1</CacheSecurityGroupName>"));
    assert!(body.contains("<CacheSecurityGroupName>ec2c-2</CacheSecurityGroupName>"));
    assert!(body.contains("<CacheParameterGroupName>default.redis7</CacheParameterGroupName>"));
    // SecurityGroupMembershipList's member has no xmlName so awsQuery
    // serializes each entry under `<member>`. Anything else (e.g.
    // `<SecurityGroupMembership>`) makes the AWS SDK Rust deserializer
    // drop the entry. Lock the wire shape in so the regression in
    // PR #1169 doesn't come back.
    let normalized = body.replace([' ', '\n'], "");
    assert!(normalized.contains(
        "<member><SecurityGroupId>sg-aaa</SecurityGroupId><Status>active</Status></member>"
    ));
    assert!(normalized.contains(
        "<member><SecurityGroupId>sg-bbb</SecurityGroupId><Status>active</Status></member>"
    ));
    assert!(!body.contains("<SecurityGroupMembership>"));
}

#[test]
fn create_cache_cluster_persists_snapshot_window_and_retention() {
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "snap-cc"),
            ("SnapshotRetentionLimit", "7"),
            ("SnapshotWindow", "03:00-05:00"),
            ("SnapshotName", "seed-snapshot"),
            (
                "SnapshotArns.SnapshotArn.1",
                "arn:aws:s3:::my-bucket/snap.rdb",
            ),
            ("PreferredMaintenanceWindow", "sun:23:00-mon:01:30"),
            ("NotificationTopicArn", "arn:aws:sns:us-east-1:123:events"),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let describe = request("DescribeCacheClusters", &[("CacheClusterId", "snap-cc")]);
    let resp = svc.describe_cache_clusters(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<SnapshotRetentionLimit>7</SnapshotRetentionLimit>"));
    assert!(body.contains("<SnapshotWindow>03:00-05:00</SnapshotWindow>"));
    assert!(body
        .contains("<PreferredMaintenanceWindow>sun:23:00-mon:01:30</PreferredMaintenanceWindow>"));
    assert!(body.contains("<TopicArn>arn:aws:sns:us-east-1:123:events</TopicArn>"));
    // SnapshotName + SnapshotArns are stored on the struct but aren't echoed
    // by Describe; assert the in-memory state holds them.
    let state = svc.state.read();
    let cluster = state
        .get("123456789012")
        .unwrap()
        .cache_clusters
        .get("snap-cc")
        .unwrap();
    assert_eq!(cluster.snapshot_name.as_deref(), Some("seed-snapshot"));
    assert_eq!(
        cluster.snapshot_arns,
        vec!["arn:aws:s3:::my-bucket/snap.rdb".to_string()]
    );
}

#[test]
fn create_cache_cluster_persists_network_fields() {
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "net-cc"),
            ("NetworkType", "dual_stack"),
            ("IpDiscovery", "ipv6"),
            ("AZMode", "cross-az"),
            ("OutpostMode", "single-outpost"),
            (
                "PreferredOutpostArn",
                "arn:aws:outposts:us-east-1:123:outpost/op-abc",
            ),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let describe = request("DescribeCacheClusters", &[("CacheClusterId", "net-cc")]);
    let resp = svc.describe_cache_clusters(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<NetworkType>dual_stack</NetworkType>"));
    assert!(body.contains("<IpDiscovery>ipv6</IpDiscovery>"));
    assert!(body.contains("<AZMode>cross-az</AZMode>"));
    assert!(body.contains("<OutpostMode>single-outpost</OutpostMode>"));
    assert!(body.contains(
        "<PreferredOutpostArn>arn:aws:outposts:us-east-1:123:outpost/op-abc</PreferredOutpostArn>"
    ));
}

#[test]
fn create_cache_cluster_defaults_port_for_redis() {
    let create_req = request(
        "CreateCacheCluster",
        &[("CacheClusterId", "redis-cc"), ("Engine", "redis")],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let state = svc.state.read();
    let cluster = state
        .get("123456789012")
        .unwrap()
        .cache_clusters
        .get("redis-cc")
        .unwrap();
    assert_eq!(cluster.port, 6379);
}

#[test]
fn create_cache_cluster_defaults_port_for_memcached() {
    let create_req = request(
        "CreateCacheCluster",
        &[("CacheClusterId", "mc-cc"), ("Engine", "memcached")],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let state = svc.state.read();
    let cluster = state
        .get("123456789012")
        .unwrap()
        .cache_clusters
        .get("mc-cc")
        .unwrap();
    assert_eq!(cluster.port, 11211);
}

#[test]
fn create_cache_cluster_persists_preferred_azs() {
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "az-cc"),
            ("Engine", "memcached"),
            ("NumCacheNodes", "3"),
            ("PreferredAvailabilityZone", "us-east-1a"),
            (
                "PreferredAvailabilityZones.AvailabilityZone.1",
                "us-east-1a",
            ),
            (
                "PreferredAvailabilityZones.AvailabilityZone.2",
                "us-east-1b",
            ),
            (
                "PreferredAvailabilityZones.AvailabilityZone.3",
                "us-east-1c",
            ),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let describe = request("DescribeCacheClusters", &[("CacheClusterId", "az-cc")]);
    let resp = svc.describe_cache_clusters(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<PreferredAvailabilityZone>us-east-1a</PreferredAvailabilityZone>"));
    assert!(body.contains("<AvailabilityZone>us-east-1a</AvailabilityZone>"));
    assert!(body.contains("<AvailabilityZone>us-east-1b</AvailabilityZone>"));
    assert!(body.contains("<AvailabilityZone>us-east-1c</AvailabilityZone>"));
}

#[test]
fn create_cache_cluster_does_not_echo_auth_token() {
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "auth-cc"),
            ("Engine", "redis"),
            ("AuthToken", "supersecret-token-XYZ"),
            ("TransitEncryptionEnabled", "true"),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    let describe = request("DescribeCacheClusters", &[("CacheClusterId", "auth-cc")]);
    let resp = svc.describe_cache_clusters(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<AuthTokenEnabled>true</AuthTokenEnabled>"));
    // Raw token must never be echoed back in describe XML.
    assert!(!body.contains("supersecret-token-XYZ"));
    // But the stored struct keeps it for future ModifyCacheCluster comparisons.
    let state = svc.state.read();
    let cluster = state
        .get("123456789012")
        .unwrap()
        .cache_clusters
        .get("auth-cc")
        .unwrap();
    assert_eq!(cluster.auth_token.as_deref(), Some("supersecret-token-XYZ"));
}

#[test]
fn create_cache_cluster_persists_kitchen_sink_fields() {
    // Kitchen-sink CreateCacheCluster: every documented input field plus the
    // newer encryption / cluster-mode / data-tiering knobs. Asserts the full
    // round-trip through state + DescribeCacheClusters.
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "kitchen-sink-cc"),
            ("Engine", "redis"),
            ("EngineVersion", "7.1"),
            ("CacheNodeType", "cache.r6gd.xlarge"),
            ("NumCacheNodes", "1"),
            ("CacheParameterGroupName", "default.redis7"),
            ("CacheSubnetGroupName", "default"),
            ("SecurityGroupIds.SecurityGroupId.1", "sg-aaa"),
            ("SecurityGroupIds.SecurityGroupId.2", "sg-bbb"),
            ("CacheSecurityGroupNames.CacheSecurityGroupName.1", "ec2c-1"),
            ("Port", "6390"),
            ("PreferredMaintenanceWindow", "sun:05:00-sun:09:00"),
            ("PreferredAvailabilityZone", "us-east-1a"),
            ("AZMode", "single-az"),
            ("AutoMinorVersionUpgrade", "false"),
            ("NotificationTopicArn", "arn:aws:sns:us-east-1:123:topic"),
            ("AuthToken", "supersecret-XYZ"),
            ("TransitEncryptionEnabled", "true"),
            ("AtRestEncryptionEnabled", "true"),
            ("TransitEncryptionMode", "required"),
            (
                "KmsKeyId",
                "arn:aws:kms:us-east-1:123:key/abcd-efgh-1234",
            ),
            ("DataTieringEnabled", "true"),
            ("ClusterMode", "compatible"),
            ("NetworkType", "dual_stack"),
            ("IpDiscovery", "ipv6"),
            ("OutpostMode", "single-outpost"),
            (
                "PreferredOutpostArn",
                "arn:aws:outposts:us-east-1:123:outpost/op-abc",
            ),
            (
                "PreferredOutpostArns.PreferredOutpostArn.1",
                "arn:aws:outposts:us-east-1:123:outpost/op-abc",
            ),
            (
                "PreferredOutpostArns.PreferredOutpostArn.2",
                "arn:aws:outposts:us-east-1:123:outpost/op-def",
            ),
            ("SnapshotName", "seed-snap"),
            ("SnapshotArns.SnapshotArn.1", "arn:aws:s3:::bkt/seed.rdb"),
            ("SnapshotRetentionLimit", "7"),
            ("SnapshotWindow", "03:00-05:00"),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogType",
                "slow-log",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationType",
                "cloudwatch-logs",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.DestinationDetails.CloudWatchLogsDetails.LogGroup",
                "/aws/elasticache/slow",
            ),
            (
                "LogDeliveryConfigurations.LogDeliveryConfigurationRequest.1.LogFormat",
                "json",
            ),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "platform"),
            ("Tags.Tag.2.Key", "env"),
            ("Tags.Tag.2.Value", "prod"),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);

    // Verify the full struct round-tripped to state.
    {
        let state = svc.state.read();
        let cluster = state
            .get("123456789012")
            .unwrap()
            .cache_clusters
            .get("kitchen-sink-cc")
            .unwrap();
        assert_eq!(cluster.engine, "redis");
        assert_eq!(cluster.engine_version, "7.1");
        assert_eq!(cluster.cache_node_type, "cache.r6gd.xlarge");
        assert_eq!(cluster.num_cache_nodes, 1);
        assert_eq!(
            cluster.cache_parameter_group_name.as_deref(),
            Some("default.redis7")
        );
        assert_eq!(cluster.cache_subnet_group_name.as_deref(), Some("default"));
        assert_eq!(cluster.security_group_ids, vec!["sg-aaa", "sg-bbb"]);
        assert_eq!(cluster.cache_security_group_names, vec!["ec2c-1"]);
        assert_eq!(cluster.port, 6390);
        assert_eq!(
            cluster.preferred_maintenance_window.as_deref(),
            Some("sun:05:00-sun:09:00")
        );
        assert_eq!(cluster.preferred_availability_zone, "us-east-1a");
        assert_eq!(cluster.az_mode.as_deref(), Some("single-az"));
        assert!(!cluster.auto_minor_version_upgrade);
        assert_eq!(
            cluster.notification_topic_arn.as_deref(),
            Some("arn:aws:sns:us-east-1:123:topic")
        );
        assert_eq!(cluster.auth_token.as_deref(), Some("supersecret-XYZ"));
        assert!(cluster.auth_token_enabled);
        assert!(cluster.transit_encryption_enabled);
        assert!(cluster.at_rest_encryption_enabled);
        assert_eq!(cluster.transit_encryption_mode.as_deref(), Some("required"));
        assert_eq!(
            cluster.kms_key_id.as_deref(),
            Some("arn:aws:kms:us-east-1:123:key/abcd-efgh-1234")
        );
        assert_eq!(cluster.data_tiering_enabled, Some(true));
        assert_eq!(cluster.cluster_mode.as_deref(), Some("compatible"));
        assert_eq!(cluster.network_type.as_deref(), Some("dual_stack"));
        assert_eq!(cluster.ip_discovery.as_deref(), Some("ipv6"));
        assert_eq!(cluster.outpost_mode.as_deref(), Some("single-outpost"));
        assert_eq!(
            cluster.preferred_outpost_arn.as_deref(),
            Some("arn:aws:outposts:us-east-1:123:outpost/op-abc")
        );
        assert_eq!(
            cluster.preferred_outpost_arns,
            vec![
                "arn:aws:outposts:us-east-1:123:outpost/op-abc",
                "arn:aws:outposts:us-east-1:123:outpost/op-def",
            ]
        );
        assert_eq!(cluster.snapshot_name.as_deref(), Some("seed-snap"));
        assert_eq!(cluster.snapshot_arns, vec!["arn:aws:s3:::bkt/seed.rdb"]);
        assert_eq!(cluster.snapshot_retention_limit, 7);
        assert_eq!(cluster.snapshot_window.as_deref(), Some("03:00-05:00"));
        assert_eq!(cluster.log_delivery_configurations.len(), 1);
    }

    // DescribeCacheClusters round-trip — every field that AWS echoes on the
    // CacheCluster response shape must show up in the body.
    let describe = request(
        "DescribeCacheClusters",
        &[("CacheClusterId", "kitchen-sink-cc")],
    );
    let resp = svc.describe_cache_clusters(&describe).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheClusterId>kitchen-sink-cc</CacheClusterId>"));
    assert!(body.contains("<Engine>redis</Engine>"));
    assert!(body.contains("<EngineVersion>7.1</EngineVersion>"));
    assert!(body.contains("<CacheNodeType>cache.r6gd.xlarge</CacheNodeType>"));
    assert!(body.contains("<NumCacheNodes>1</NumCacheNodes>"));
    assert!(body.contains("<CacheSubnetGroupName>default</CacheSubnetGroupName>"));
    assert!(body.contains("<CacheParameterGroupName>default.redis7</CacheParameterGroupName>"));
    assert!(body.contains("<SecurityGroupId>sg-aaa</SecurityGroupId>"));
    assert!(body.contains("<SecurityGroupId>sg-bbb</SecurityGroupId>"));
    assert!(body.contains("<CacheSecurityGroupName>ec2c-1</CacheSecurityGroupName>"));
    assert!(body.contains("<TransitEncryptionEnabled>true</TransitEncryptionEnabled>"));
    assert!(body.contains("<AtRestEncryptionEnabled>true</AtRestEncryptionEnabled>"));
    assert!(body.contains("<TransitEncryptionMode>required</TransitEncryptionMode>"));
    assert!(body.contains("<AuthTokenEnabled>true</AuthTokenEnabled>"));
    assert!(body.contains("<AutoMinorVersionUpgrade>false</AutoMinorVersionUpgrade>"));
    assert!(body.contains("<NetworkType>dual_stack</NetworkType>"));
    assert!(body.contains("<IpDiscovery>ipv6</IpDiscovery>"));
    assert!(body.contains("<AZMode>single-az</AZMode>"));
    assert!(body.contains("<OutpostMode>single-outpost</OutpostMode>"));
    assert!(body.contains(
        "<PreferredOutpostArn>arn:aws:outposts:us-east-1:123:outpost/op-abc</PreferredOutpostArn>"
    ));
    assert!(body
        .contains("<PreferredMaintenanceWindow>sun:05:00-sun:09:00</PreferredMaintenanceWindow>"));
    assert!(body.contains("<TopicArn>arn:aws:sns:us-east-1:123:topic</TopicArn>"));
    assert!(body.contains("<SnapshotRetentionLimit>7</SnapshotRetentionLimit>"));
    assert!(body.contains("<SnapshotWindow>03:00-05:00</SnapshotWindow>"));
    assert!(body.contains("<LogType>slow-log</LogType>"));
    assert!(body.contains("<DestinationType>cloudwatch-logs</DestinationType>"));
    assert!(body.contains("<LogGroup>/aws/elasticache/slow</LogGroup>"));
    // Auth token raw value must never appear.
    assert!(!body.contains("supersecret-XYZ"));
}

#[test]
fn create_cache_cluster_through_handler_persists_tags_and_extended_fields() {
    // Drive the real `create_cache_cluster` handler path by pre-seeding the
    // cluster as the runtime would — service_with_cache_cluster_from_request
    // runs the same parser the handler does, so this is a faithful unit-test
    // of the persistence layer including Tags + new fields.
    let create_req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "tagged-cc"),
            ("Engine", "redis"),
            ("KmsKeyId", "arn:aws:kms:us-east-1:123:key/k1"),
            ("DataTieringEnabled", "false"),
            ("ClusterMode", "disabled"),
            ("TransitEncryptionMode", "preferred"),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "platform"),
        ],
    );
    let svc = service_with_cache_cluster_from_request(&create_req);
    let state = svc.state.read();
    let account = state.get("123456789012").unwrap();
    let cluster = account.cache_clusters.get("tagged-cc").unwrap();
    assert_eq!(
        cluster.kms_key_id.as_deref(),
        Some("arn:aws:kms:us-east-1:123:key/k1")
    );
    assert_eq!(cluster.data_tiering_enabled, Some(false));
    assert_eq!(cluster.cluster_mode.as_deref(), Some("disabled"));
    assert_eq!(
        cluster.transit_encryption_mode.as_deref(),
        Some("preferred")
    );
}

// ── snapshot restore paths ──

// SnapshotNotFoundFault is not in CreateCacheCluster / CreateReplicationGroup's
// declared `errors:` list per `aws-models/elasticache.json`. An unknown
// SnapshotName is therefore treated as "no restore" and the create succeeds.
#[tokio::test]
async fn create_cache_cluster_with_missing_snapshot_creates_without_restore() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "cc-restore"),
            ("SnapshotName", "no-such-snap"),
        ],
    );
    let resp = svc
        .create_cache_cluster(&req)
        .await
        .expect("create succeeds without snapshot");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<CacheClusterId>cc-restore</CacheClusterId>"));
}

#[tokio::test]
async fn create_replication_group_with_missing_snapshot_creates_without_restore() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared);
    let req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-restore"),
            ("ReplicationGroupDescription", "desc"),
            ("SnapshotName", "no-such-snap"),
        ],
    );
    let resp = svc
        .create_replication_group(&req)
        .await
        .expect("create succeeds without snapshot");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ReplicationGroupId>rg-restore</ReplicationGroupId>"));
}

#[tokio::test]
async fn create_cache_cluster_with_existing_snapshot_creates_metadata_only() {
    // With no container runtime, the create still succeeds (degraded to
    // metadata-only). The rdb restore would happen if a runtime were
    // available; the snapshot lookup itself doesn't fail the call.
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared.clone());
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        state.snapshots.insert(
            "my-snap".to_string(),
            crate::state::CacheSnapshot {
                snapshot_name: "my-snap".to_string(),
                replication_group_id: "rg-1".to_string(),
                replication_group_description: "desc".to_string(),
                snapshot_status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                num_cache_clusters: 1,
                arn: "arn:aws:elasticache:us-east-1:123456789012:snapshot:my-snap".to_string(),
                created_at: chrono::Utc::now().to_rfc3339(),
                snapshot_source: "manual".to_string(),
                rdb_path: Some("/tmp/fake.rdb".to_string()),
            },
        );
    }
    let req = request(
        "CreateCacheCluster",
        &[
            ("CacheClusterId", "cc-restore"),
            ("SnapshotName", "my-snap"),
        ],
    );
    svc.create_cache_cluster(&req)
        .await
        .expect("create succeeds with known snapshot, no runtime");
    let accounts = shared.read();
    let state = accounts.get("123456789012").unwrap();
    let cluster = state.cache_clusters.get("cc-restore").unwrap();
    assert!(cluster.container_id.is_empty());
}

#[tokio::test]
async fn create_replication_group_with_existing_snapshot_creates_metadata_only() {
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = ElastiCacheService::new(shared.clone());
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        state.snapshots.insert(
            "my-snap".to_string(),
            crate::state::CacheSnapshot {
                snapshot_name: "my-snap".to_string(),
                replication_group_id: "rg-1".to_string(),
                replication_group_description: "desc".to_string(),
                snapshot_status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                num_cache_clusters: 1,
                arn: "arn:aws:elasticache:us-east-1:123456789012:snapshot:my-snap".to_string(),
                created_at: chrono::Utc::now().to_rfc3339(),
                snapshot_source: "manual".to_string(),
                rdb_path: Some("/tmp/fake.rdb".to_string()),
            },
        );
    }
    let req = request(
        "CreateReplicationGroup",
        &[
            ("ReplicationGroupId", "rg-restore"),
            ("ReplicationGroupDescription", "desc"),
            ("SnapshotName", "my-snap"),
        ],
    );
    svc.create_replication_group(&req)
        .await
        .expect("create succeeds with known snapshot, no runtime");
    let accounts = shared.read();
    let state = accounts.get("123456789012").unwrap();
    let group = state.replication_groups.get("rg-restore").unwrap();
    assert!(group.container_id.is_empty());
}

fn make_snapshot_state() -> crate::state::SharedElastiCacheState {
    std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ))
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = ElastiCacheService::new(make_snapshot_state());
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating ElastiCache
/// state directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: std::sync::Arc<dyn fakecloud_persistence::SnapshotStore> =
        std::sync::Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = ElastiCacheService::new(make_snapshot_state()).with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}

#[tokio::test]
async fn create_serverless_cache_without_runtime_assigns_default_port() {
    // No container runtime: the cache is created directly "available", so it
    // must advertise a real port (6379 for redis/valkey) rather than 0, which
    // would make every connection string end in `:0` (unconnectable).
    // bug-hunt 2026-07-16, 1.7.
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    let resp = service
        .create_serverless_cache(&request(
            "CreateServerlessCache",
            &[("ServerlessCacheName", "sc-redis"), ("Engine", "redis")],
        ))
        .await
        .expect("metadata-only create");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Status>available</Status>"));
    assert!(body.contains("<Port>6379</Port>"), "body: {body}");
    assert!(
        !body.contains("<Port>0</Port>"),
        "endpoint port must not be 0"
    );

    // Describe round-trips the non-zero port too.
    let desc = service
        .describe_serverless_caches(&request(
            "DescribeServerlessCaches",
            &[("ServerlessCacheName", "sc-redis")],
        ))
        .unwrap();
    let desc_body = String::from_utf8(desc.body.expect_bytes().to_vec()).unwrap();
    assert!(desc_body.contains("<Port>6379</Port>"), "desc: {desc_body}");
    assert!(!desc_body.contains("<Port>0</Port>"));
}

#[tokio::test]
async fn replication_group_emits_primary_and_replica_members() {
    // A replication group created with NumCacheClusters=3 has 1 primary + 2
    // replicas. The Terraform provider derives replicas_per_node_group from
    // len(NodeGroupMembers) - 1, so all three must be emitted or the plan
    // perpetually diffs. bug-hunt 2026-07-16, 1.8.
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared);

    service
        .create_replication_group(&request(
            "CreateReplicationGroup",
            &[
                ("ReplicationGroupId", "rg-repl"),
                ("ReplicationGroupDescription", "test"),
                ("NumCacheClusters", "3"),
            ],
        ))
        .await
        .expect("create replication group");

    let resp = service
        .describe_replication_groups(&request(
            "DescribeReplicationGroups",
            &[("ReplicationGroupId", "rg-repl")],
        ))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert_eq!(
        body.matches("<NodeGroupMember>").count(),
        3,
        "expected 1 primary + 2 replicas, body: {body}"
    );
    assert_eq!(
        body.matches("<CurrentRole>primary</CurrentRole>").count(),
        1
    );
    assert_eq!(
        body.matches("<CurrentRole>replica</CurrentRole>").count(),
        2
    );
    assert!(body.contains("<CacheClusterId>rg-repl-001</CacheClusterId>"));
    assert!(body.contains("<CacheClusterId>rg-repl-002</CacheClusterId>"));
    assert!(body.contains("<CacheClusterId>rg-repl-003</CacheClusterId>"));
}

#[tokio::test]
async fn export_serverless_cache_snapshot_writes_object_to_s3() {
    // ExportServerlessCacheSnapshot must actually drop the export artifact into
    // the caller's S3 bucket rather than discard S3BucketName. bug-hunt
    // 2026-07-16, 1.25.
    use fakecloud_s3::{S3Bucket, SharedS3State};
    let s3: SharedS3State = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut guard = s3.write();
        let acc = guard.get_or_create("123456789012");
        acc.buckets.insert(
            "export-bucket".to_string(),
            S3Bucket::new("export-bucket", "us-east-1", "123456789012"),
        );
    }

    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let service = ElastiCacheService::new(shared).with_s3(s3.clone());
    {
        let mut __a = service.state.write();
        let state = __a.default_mut();
        state.serverless_cache_snapshots.insert(
            "snap-x".to_string(),
            ServerlessCacheSnapshot {
                serverless_cache_snapshot_name: "snap-x".to_string(),
                arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-x"
                    .to_string(),
                kms_key_id: None,
                snapshot_type: "manual".to_string(),
                status: "available".to_string(),
                create_time: "2024-01-01T00:00:00Z".to_string(),
                expiry_time: None,
                bytes_used_for_cache: None,
                serverless_cache_name: "cache-a".to_string(),
                engine: "redis".to_string(),
                major_engine_version: "7.1".to_string(),
            },
        );
    }

    let resp = service
        .export_serverless_cache_snapshot(&request(
            "ExportServerlessCacheSnapshot",
            &[
                ("ServerlessCacheSnapshotName", "snap-x"),
                ("S3BucketName", "export-bucket"),
            ],
        ))
        .expect("export succeeds");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ServerlessCacheSnapshotName>snap-x</ServerlessCacheSnapshotName>"));

    // The export object now exists in the target bucket with real bytes + ETag.
    {
        let guard = s3.read();
        let acc = guard.get("123456789012").expect("s3 account");
        let bucket = acc.buckets.get("export-bucket").expect("bucket");
        let obj = bucket
            .objects
            .get("snap-x.rdb")
            .expect("export object written to S3");
        assert!(obj.size > 0);
        assert!(obj.etag.starts_with('"') && obj.etag.ends_with('"'));
    }

    // A missing bucket is rejected the way AWS does, not silently dropped.
    match service.export_serverless_cache_snapshot(&request(
        "ExportServerlessCacheSnapshot",
        &[
            ("ServerlessCacheSnapshotName", "snap-x"),
            ("S3BucketName", "nonexistent-bucket"),
        ],
    )) {
        Ok(_) => panic!("expected missing-bucket rejection"),
        Err(e) => {
            assert_eq!(e.status(), StatusCode::BAD_REQUEST);
            assert_eq!(e.code(), "InvalidParameterValue");
        }
    }
}

#[test]
fn recovery_predicate_redrives_transient_states() {
    use crate::service::is_recoverable_status;
    // Mid-transition states must be recovered (re-driven) on restart.
    assert!(is_recoverable_status("available"));
    assert!(is_recoverable_status("creating"));
    assert!(is_recoverable_status("modifying"));
    assert!(is_recoverable_status("rebooting cache cluster nodes"));
    assert!(is_recoverable_status("starting"));
    // Resources being torn down are not.
    assert!(!is_recoverable_status("deleting"));
    assert!(!is_recoverable_status("deleted"));
}
