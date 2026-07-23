//! RDS `subnet_groups` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    pub(super) fn create_db_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = required_query_param(request, "DBSubnetGroupName")?;
        let db_subnet_group_description =
            required_query_param(request, "DBSubnetGroupDescription")?;
        let subnet_ids = parse_subnet_ids(request)?;

        if subnet_ids.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.subnet_groups.contains_key(&db_subnet_group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DBSubnetGroupAlreadyExists",
                format!("DBSubnetGroup {db_subnet_group_name} already exists."),
            ));
        }

        let vpc_id = format!("vpc-{}", uuid::Uuid::new_v4().simple());
        // We don't track real VPC subnet -> AZ mappings, so synthesize
        // one AZ per subnet by hashing the subnet id. Two distinct
        // subnet ids land in the same AZ only on hash collision, which
        // makes the uniqueness check meaningful (the previous version
        // derived AZ from index and was always unique by construction).
        // Distinct subnet ids land in distinct AZs (each one gets its
        // own letter) so the uniqueness check rejects only the case
        // where the caller actually repeated a subnet id. We don't
        // simulate real VPC subnet -> AZ mappings, so the previous
        // hash-to-6-buckets approach produced spurious collisions for
        // unrelated subnets.
        let mut subnet_availability_zones: Vec<String> = Vec::with_capacity(subnet_ids.len());
        let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        let region = &state.region;
        for sid in &subnet_ids {
            let next = seen.len();
            let idx = *seen.entry(sid.as_str()).or_insert(next);
            let bucket = (idx % 26) as u8;
            subnet_availability_zones.push(format!("{}{}", region, char::from(b'a' + bucket)));
        }

        let unique_azs: std::collections::HashSet<_> = subnet_availability_zones.iter().collect();
        if unique_azs.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        let db_subnet_group_arn =
            state.db_subnet_group_arn(request.region.as_str(), &db_subnet_group_name);
        let tags = parse_tags(request)?;

        let subnet_group = DbSubnetGroup {
            db_subnet_group_name: db_subnet_group_name.clone(),
            db_subnet_group_arn,
            db_subnet_group_description,
            vpc_id,
            subnet_ids,
            subnet_availability_zones,
            tags,
        };

        state
            .subnet_groups
            .insert(db_subnet_group_name, subnet_group.clone());

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateDBSubnetGroup",
                RDS_NS,
                &format!(
                    "<DBSubnetGroup>{}</DBSubnetGroup>",
                    db_subnet_group_xml(&subnet_group)
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_db_subnet_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = optional_query_param(request, "DBSubnetGroupName");
        let marker = optional_query_param(request, "Marker");
        let max_records = optional_query_param(request, "MaxRecords");

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        // If specific subnet group requested, return just that one (no pagination)
        if let Some(name) = db_subnet_group_name {
            let sg = state.subnet_groups.get(&name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBSubnetGroupNotFoundFault",
                    format!("DBSubnetGroup {} not found.", name),
                )
            })?;

            return Ok(AwsResponse::xml(
                StatusCode::OK,
                query_response_xml(
                    "DescribeDBSubnetGroups",
                    RDS_NS,
                    &format!(
                        "<DBSubnetGroups><DBSubnetGroup>{}</DBSubnetGroup></DBSubnetGroups>",
                        db_subnet_group_xml(sg)
                    ),
                    &request.request_id,
                ),
            ));
        }

        // Get all subnet groups sorted by name
        let mut subnet_groups: Vec<DbSubnetGroup> = state.subnet_groups.values().cloned().collect();
        subnet_groups.sort_by(|a, b| a.db_subnet_group_name.cmp(&b.db_subnet_group_name));

        // Apply pagination
        let paginated = paginate(subnet_groups, marker, max_records, |sg| {
            &sg.db_subnet_group_name
        })?;

        let marker_xml = paginated
            .next_marker
            .as_ref()
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(m)))
            .unwrap_or_default();

        let body = paginated
            .items
            .iter()
            .map(|sg| format!("<DBSubnetGroup>{}</DBSubnetGroup>", db_subnet_group_xml(sg)))
            .collect::<Vec<_>>()
            .join("");

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBSubnetGroups",
                RDS_NS,
                &format!("<DBSubnetGroups>{}</DBSubnetGroups>{}", body, marker_xml),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_db_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = required_query_param(request, "DBSubnetGroupName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.subnet_groups.remove(&db_subnet_group_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "DBSubnetGroupNotFoundFault",
                format!("DBSubnetGroup {db_subnet_group_name} not found."),
            ));
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("DeleteDBSubnetGroup", RDS_NS, "", &request.request_id),
        ))
    }

    pub(super) fn modify_db_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_subnet_group_name = required_query_param(request, "DBSubnetGroupName")?;
        let subnet_ids = parse_subnet_ids(request)?;

        if subnet_ids.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let region = state.region.clone();

        let subnet_group = state
            .subnet_groups
            .get_mut(&db_subnet_group_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBSubnetGroupNotFoundFault",
                    format!("DBSubnetGroup {db_subnet_group_name} not found."),
                )
            })?;

        let mut subnet_availability_zones: Vec<String> = Vec::with_capacity(subnet_ids.len());
        let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for sid in &subnet_ids {
            let next = seen.len();
            let idx = *seen.entry(sid.as_str()).or_insert(next);
            let bucket = (idx % 26) as u8;
            subnet_availability_zones.push(format!("{}{}", region, char::from(b'a' + bucket)));
        }

        let unique_azs: std::collections::HashSet<_> = subnet_availability_zones.iter().collect();
        if unique_azs.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }

        subnet_group.subnet_ids = subnet_ids;
        subnet_group.subnet_availability_zones = subnet_availability_zones;
        if let Some(description) = optional_query_param(request, "DBSubnetGroupDescription") {
            subnet_group.db_subnet_group_description = description;
        }

        let subnet_group_clone = subnet_group.clone();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyDBSubnetGroup",
                RDS_NS,
                &format!(
                    "<DBSubnetGroup>{}</DBSubnetGroup>",
                    db_subnet_group_xml(&subnet_group_clone)
                ),
                &request.request_id,
            ),
        ))
    }
}
