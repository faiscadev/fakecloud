//! Amazon EC2 implementation for FakeCloud.
//!
//! EC2 speaks the `ec2Query` protocol (form-encoded requests, flattened-XML
//! responses — see [`fakecloud_aws::ec2query`]). This crate is built out across
//! many batches toward full operation parity; the foundation here provides the
//! service scaffold, shared `Filter`/pagination infrastructure, the resource
//! tagging subsystem, and the region/AZ/account-attribute describe primitives
//! that almost every SDK client calls implicitly.

pub mod cfn_provision;
pub mod defaults;
pub mod runtime;
pub mod service;
pub mod service_helpers;
pub mod state;

pub use runtime::Ec2Runtime;
pub use service::Ec2Service;
pub use state::{Ec2Snapshot, Ec2State, SharedEc2State, EC2_SNAPSHOT_SCHEMA_VERSION};

/// Shared test helpers for the in-crate handler unit tests.
#[cfg(test)]
pub(crate) mod test_support {
    use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

    /// Extract the error from a handler result (AwsResponse is not `Debug`, so
    /// `unwrap_err` cannot be used directly).
    pub(crate) fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error, got Ok"),
            Err(e) => e,
        }
    }

    /// Insert a minimal running instance into the default test account so
    /// handlers that look the instance up have something to find.
    pub(crate) fn seed_instance(svc: &crate::service::Ec2Service, id: &str) {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("000000000000");
        let inst = crate::state::Instance {
            instance_id: id.into(),
            image_id: "ami-1".into(),
            instance_type: "t3.micro".into(),
            state_code: 16,
            state_name: "running".into(),
            private_ip: "10.0.0.5".into(),
            public_ip: None,
            subnet_id: Some("subnet-1".into()),
            vpc_id: Some("vpc-1".into()),
            key_name: None,
            security_group_ids: vec![],
            reservation_id: "r-1".into(),
            ami_launch_index: 0,
            monitoring: false,
            az: "us-east-1a".into(),
            launch_time: "2024-01-01T00:00:00.000Z".into(),
            container_id: None,
            disable_api_termination: false,
            disable_api_stop: false,
            source_dest_check: true,
            ebs_optimized: false,
            instance_initiated_shutdown_behavior: "stop".into(),
            user_data: None,
            metadata_options: Default::default(),
            cpu_options: None,
            bandwidth_weighting: None,
            maintenance_options: Default::default(),
            placement_tenancy: None,
            placement_affinity: None,
            placement_group_name: None,
            private_dns_hostname_type: None,
            enable_resource_name_dns_a_record: false,
            enable_resource_name_dns_aaaa_record: false,
        };
        state.instances.insert(id.to_string(), inst);
    }

    /// The `<associationId>` of an IAM instance-profile association response.
    pub(crate) fn assoc_id_of(xml: &str) -> String {
        xml.split("<associationId>")
            .nth(1)
            .and_then(|s| s.split("</associationId>").next())
            .expect("associationId in response")
            .to_string()
    }

    /// Build a minimal query-protocol [`AwsRequest`] for handler unit tests.
    pub(crate) fn ec2_request(action: &str, query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }
}
