//! RDS `subnet_groups` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    /// Resolve a DB subnet group's VPC and per-subnet Availability Zones from
    /// the EC2 subnets: every subnet must exist and share one VPC
    /// (`InvalidSubnet`), and they must span at least two Availability Zones.
    /// Without EC2 state (memory-only unit tests) each distinct subnet id is
    /// given its own zone and the group has no VPC.
    pub(super) fn place_subnet_group(
        &self,
        request: &AwsRequest,
        subnet_ids: &[String],
    ) -> Result<(String, Vec<String>), AwsServiceError> {
        let (vpc_id, zones) = match &self.ec2_state {
            Some(ec2) => {
                let placed = fakecloud_ec2::vpc_lookup::resolve_subnet_group(
                    ec2,
                    &request.account_id,
                    &request.region,
                    subnet_ids,
                )
                .map_err(|e| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidSubnet",
                        e.message(),
                    )
                })?;
                let zones = placed.availability_zones();
                (placed.vpc_id, zones)
            }
            None => {
                let mut seen: std::collections::HashMap<&str, usize> =
                    std::collections::HashMap::new();
                let zones = subnet_ids
                    .iter()
                    .map(|sid| {
                        let next = seen.len();
                        let idx = *seen.entry(sid.as_str()).or_insert(next);
                        format!("{}{}", request.region, char::from(b'a' + (idx % 26) as u8))
                    })
                    .collect();
                (String::new(), zones)
            }
        };
        let unique: std::collections::HashSet<&String> = zones.iter().collect();
        if unique.len() < 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupDoesNotCoverEnoughAZs",
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones.",
            ));
        }
        Ok((vpc_id, zones))
    }

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
                StatusCode::BAD_REQUEST,
                "DBSubnetGroupAlreadyExists",
                format!("DBSubnetGroup {db_subnet_group_name} already exists."),
            ));
        }

        let (vpc_id, subnet_availability_zones) = self.place_subnet_group(request, &subnet_ids)?;

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

        let (vpc_id, subnet_availability_zones) = self.place_subnet_group(request, &subnet_ids)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

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

        if let Some(ec2) = &self.ec2_state {
            if !fakecloud_ec2::vpc_lookup::subnet_group_vpc_change_allowed(
                ec2,
                &request.account_id,
                &request.region,
                &subnet_group.vpc_id,
                &vpc_id,
            ) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidSubnet",
                    fakecloud_ec2::vpc_lookup::SUBNET_GROUP_VPC_CHANGE_MESSAGE,
                ));
            }
        }
        subnet_group.vpc_id = vpc_id;
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
