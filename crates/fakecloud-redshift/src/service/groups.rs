//! Cluster parameter groups, subnet groups, and security groups.

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::helpers::*;
use super::RedshiftService;
use crate::state::{
    ClusterParameterGroup, ClusterSecurityGroup, ClusterSubnetGroup, Ec2SecurityGroup, IpRange,
    Parameter, Subnet,
};

fn default_parameters() -> Vec<Parameter> {
    vec![
        Parameter {
            parameter_name: "max_cursor_result_set_size".to_string(),
            parameter_value: "default".to_string(),
            description: "Sets the maximum result set size".to_string(),
            source: "engine-default".to_string(),
            data_type: "integer".to_string(),
            allowed_values: "0-14400000".to_string(),
            apply_type: "static".to_string(),
            is_modifiable: true,
        },
        Parameter {
            parameter_name: "require_ssl".to_string(),
            parameter_value: "false".to_string(),
            description: "Require an SSL connection".to_string(),
            source: "engine-default".to_string(),
            data_type: "boolean".to_string(),
            allowed_values: "true,false".to_string(),
            apply_type: "static".to_string(),
            is_modifiable: true,
        },
    ]
}

fn render_parameter(p: &Parameter) -> String {
    format!(
        "<Parameter><ParameterName>{name}</ParameterName><ParameterValue>{value}</ParameterValue>\
         <Description>{desc}</Description><Source>{source}</Source><DataType>{dt}</DataType>\
         <AllowedValues>{allowed}</AllowedValues><ApplyType>{apply}</ApplyType>\
         <IsModifiable>{modifiable}</IsModifiable></Parameter>",
        name = xml_escape(&p.parameter_name),
        value = xml_escape(&p.parameter_value),
        desc = xml_escape(&p.description),
        source = xml_escape(&p.source),
        dt = xml_escape(&p.data_type),
        allowed = xml_escape(&p.allowed_values),
        apply = xml_escape(&p.apply_type),
        modifiable = p.is_modifiable,
    )
}

fn render_parameter_group(g: &ClusterParameterGroup) -> String {
    format!(
        "<ParameterGroupName>{name}</ParameterGroupName><ParameterGroupFamily>{family}</ParameterGroupFamily>\
         <Description>{desc}</Description>{tags}",
        name = xml_escape(&g.parameter_group_name),
        family = xml_escape(&g.parameter_group_family),
        desc = xml_escape(&g.description),
        tags = render_tags(&g.tags),
    )
}

fn parse_parameters(req: &AwsRequest) -> Vec<Parameter> {
    // AWS Query serializes the `ParametersList` with its `xmlName` member
    // wrapper (`Parameter`), so the SDK sends `Parameters.Parameter.N.*`.
    // Accept the plain `.member.` form too for hand-rolled callers.
    for wrapper in ["Parameters.Parameter", "Parameters.member"] {
        let parsed = parse_parameters_with(req, wrapper);
        if !parsed.is_empty() {
            return parsed;
        }
    }
    Vec::new()
}

fn parse_parameters_with(req: &AwsRequest, wrapper: &str) -> Vec<Parameter> {
    let mut out = Vec::new();
    for n in 1..=100 {
        let prefix = format!("{wrapper}.{n}");
        match req.query_params.get(&format!("{prefix}.ParameterName")) {
            Some(name) => out.push(Parameter {
                parameter_name: name.clone(),
                parameter_value: req
                    .query_params
                    .get(&format!("{prefix}.ParameterValue"))
                    .cloned()
                    .unwrap_or_default(),
                description: req
                    .query_params
                    .get(&format!("{prefix}.Description"))
                    .cloned()
                    .unwrap_or_default(),
                source: "user".to_string(),
                data_type: req
                    .query_params
                    .get(&format!("{prefix}.DataType"))
                    .cloned()
                    .unwrap_or_else(|| "string".to_string()),
                allowed_values: req
                    .query_params
                    .get(&format!("{prefix}.AllowedValues"))
                    .cloned()
                    .unwrap_or_default(),
                apply_type: req
                    .query_params
                    .get(&format!("{prefix}.ApplyType"))
                    .cloned()
                    .unwrap_or_else(|| "static".to_string()),
                is_modifiable: true,
            }),
            None => break,
        }
    }
    out
}

fn render_subnet_group(g: &ClusterSubnetGroup) -> String {
    let subnets: String = g
        .subnets
        .iter()
        .map(|s| {
            format!(
                "<Subnet><SubnetIdentifier>{id}</SubnetIdentifier>\
                 <SubnetAvailabilityZone><Name>{az}</Name></SubnetAvailabilityZone>\
                 <SubnetStatus>{status}</SubnetStatus></Subnet>",
                id = xml_escape(&s.subnet_identifier),
                az = xml_escape(&s.subnet_availability_zone),
                status = xml_escape(&s.subnet_status),
            )
        })
        .collect();
    format!(
        "<ClusterSubnetGroupName>{name}</ClusterSubnetGroupName><Description>{desc}</Description>\
         <VpcId>{vpc}</VpcId><SubnetGroupStatus>{status}</SubnetGroupStatus>\
         <Subnets>{subnets}</Subnets>{tags}",
        name = xml_escape(&g.cluster_subnet_group_name),
        desc = xml_escape(&g.description),
        vpc = xml_escape(&g.vpc_id),
        status = xml_escape(&g.subnet_group_status),
        tags = render_tags(&g.tags),
    )
}

fn render_security_group(g: &ClusterSecurityGroup) -> String {
    let ec2: String = g
        .ec2_security_groups
        .iter()
        .map(|e| {
            format!(
                "<EC2SecurityGroup><Status>{status}</Status><EC2SecurityGroupName>{name}</EC2SecurityGroupName>{owner}</EC2SecurityGroup>",
                status = xml_escape(&e.status),
                name = xml_escape(&e.ec2_security_group_name),
                owner = opt_elem("EC2SecurityGroupOwnerId", e.ec2_security_group_owner_id.as_deref()),
            )
        })
        .collect();
    let ip: String = g
        .ip_ranges
        .iter()
        .map(|r| {
            format!(
                "<IPRange><Status>{status}</Status><CIDRIP>{cidr}</CIDRIP></IPRange>",
                status = xml_escape(&r.status),
                cidr = xml_escape(&r.cidrip),
            )
        })
        .collect();
    format!(
        "<ClusterSecurityGroupName>{name}</ClusterSecurityGroupName><Description>{desc}</Description>\
         <EC2SecurityGroups>{ec2}</EC2SecurityGroups><IPRanges>{ip}</IPRanges>{tags}",
        name = xml_escape(&g.cluster_security_group_name),
        desc = xml_escape(&g.description),
        tags = render_tags(&g.tags),
    )
}

impl RedshiftService {
    // ── Parameter groups ──────────────────────────────────────────
    pub(super) fn create_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ParameterGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.parameter_groups.contains_key(&name) {
            return Err(parameter_group_already_exists(&name));
        }
        let g = ClusterParameterGroup {
            parameter_group_name: name.clone(),
            parameter_group_family: param(req, "ParameterGroupFamily").unwrap_or_default(),
            description: param(req, "Description").unwrap_or_default(),
            parameters: default_parameters(),
            tags: parse_tags(req),
        };
        acct.parameter_groups.insert(name, g.clone());
        Ok(xml_resp(
            "CreateClusterParameterGroup",
            format!(
                "<ClusterParameterGroup>{}</ClusterParameterGroup>",
                render_parameter_group(&g)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_parameter_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let acct = guard.accounts.get(&req.account_id);
        let all: Vec<ClusterParameterGroup> = match (param(req, "ParameterGroupName"), acct) {
            (Some(n), Some(a)) => match a.parameter_groups.get(&n) {
                Some(g) => vec![g.clone()],
                None => return Err(parameter_group_not_found(&n)),
            },
            (Some(n), None) => return Err(parameter_group_not_found(&n)),
            (None, Some(a)) => a.parameter_groups.values().cloned().collect(),
            (None, None) => Vec::new(),
        };
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|g| {
                format!(
                    "<ClusterParameterGroup>{}</ClusterParameterGroup>",
                    render_parameter_group(g)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeClusterParameterGroups",
            format!(
                "{}<ParameterGroups>{inner}</ParameterGroups>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ParameterGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.parameter_groups.remove(&name).is_none() {
            return Err(parameter_group_not_found(&name));
        }
        Ok(xml_metadata_only(
            "DeleteClusterParameterGroup",
            &req.request_id,
        ))
    }

    pub(super) fn modify_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ParameterGroupName").unwrap_or_default();
        let updates = parse_parameters(req);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let g = acct
            .parameter_groups
            .get_mut(&name)
            .ok_or_else(|| parameter_group_not_found(&name))?;
        for u in updates {
            if let Some(existing) = g
                .parameters
                .iter_mut()
                .find(|p| p.parameter_name == u.parameter_name)
            {
                existing.parameter_value = u.parameter_value;
                existing.source = "user".to_string();
            } else {
                g.parameters.push(u);
            }
        }
        Ok(xml_resp(
            "ModifyClusterParameterGroup",
            format!(
                "<ParameterGroupName>{}</ParameterGroupName><ParameterGroupStatus>Your parameter group has been updated.</ParameterGroupStatus>",
                xml_escape(&name)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn reset_cluster_parameter_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ParameterGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let g = acct
            .parameter_groups
            .get_mut(&name)
            .ok_or_else(|| parameter_group_not_found(&name))?;
        g.parameters = default_parameters();
        Ok(xml_resp(
            "ResetClusterParameterGroup",
            format!(
                "<ParameterGroupName>{}</ParameterGroupName><ParameterGroupStatus>Your parameter group has been reset.</ParameterGroupStatus>",
                xml_escape(&name)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ParameterGroupName").unwrap_or_default();
        let source = param(req, "Source");
        let guard = self.state.read();
        let params = guard
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.parameter_groups.get(&name))
            .map(|g| g.parameters.clone())
            .ok_or_else(|| parameter_group_not_found(&name))?;
        // Honor the `Source` filter (e.g. `user` vs `engine-default`) the way
        // real Redshift does: DescribeClusterParameters with Source=user returns
        // only parameters the caller has explicitly modified. The Terraform
        // provider relies on this to avoid perpetual drift on default params.
        let params: Vec<Parameter> = match source.as_deref() {
            Some(s) => params.into_iter().filter(|p| p.source == s).collect(),
            None => params,
        };
        let inner: String = params.iter().map(render_parameter).collect();
        Ok(xml_resp(
            "DescribeClusterParameters",
            format!("<Parameters>{inner}</Parameters>"),
            &req.request_id,
        ))
    }

    pub(super) fn describe_default_cluster_parameters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let family = param(req, "ParameterGroupFamily").unwrap_or_default();
        let inner: String = default_parameters().iter().map(render_parameter).collect();
        Ok(xml_resp(
            "DescribeDefaultClusterParameters",
            format!(
                "<DefaultClusterParameters><ParameterGroupFamily>{}</ParameterGroupFamily><Parameters>{inner}</Parameters></DefaultClusterParameters>",
                xml_escape(&family)
            ),
            &req.request_id,
        ))
    }

    // ── Subnet groups ─────────────────────────────────────────────
    pub(super) fn create_cluster_subnet_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ClusterSubnetGroupName").unwrap_or_default();
        let subnet_ids = member_list(req, "SubnetIds", "SubnetIdentifier");
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.subnet_groups.contains_key(&name) {
            return Err(subnet_group_already_exists(&name));
        }
        let g = ClusterSubnetGroup {
            cluster_subnet_group_name: name.clone(),
            description: param(req, "Description").unwrap_or_default(),
            vpc_id: "vpc-fakecloud".to_string(),
            subnet_group_status: "Complete".to_string(),
            subnets: subnet_ids
                .into_iter()
                .map(|id| Subnet {
                    subnet_identifier: id,
                    subnet_availability_zone: format!("{}a", req.region),
                    subnet_status: "Active".to_string(),
                })
                .collect(),
            tags: parse_tags(req),
        };
        acct.subnet_groups.insert(name, g.clone());
        Ok(xml_resp(
            "CreateClusterSubnetGroup",
            format!(
                "<ClusterSubnetGroup>{}</ClusterSubnetGroup>",
                render_subnet_group(&g)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_subnet_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let acct = guard.accounts.get(&req.account_id);
        let all: Vec<ClusterSubnetGroup> = match (param(req, "ClusterSubnetGroupName"), acct) {
            (Some(n), Some(a)) => match a.subnet_groups.get(&n) {
                Some(g) => vec![g.clone()],
                None => return Err(subnet_group_not_found(&n)),
            },
            (Some(n), None) => return Err(subnet_group_not_found(&n)),
            (None, Some(a)) => a.subnet_groups.values().cloned().collect(),
            (None, None) => Vec::new(),
        };
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|g| {
                format!(
                    "<ClusterSubnetGroup>{}</ClusterSubnetGroup>",
                    render_subnet_group(g)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeClusterSubnetGroups",
            format!(
                "{}<ClusterSubnetGroups>{inner}</ClusterSubnetGroups>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_cluster_subnet_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ClusterSubnetGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.subnet_groups.remove(&name).is_none() {
            return Err(subnet_group_not_found(&name));
        }
        Ok(xml_metadata_only(
            "DeleteClusterSubnetGroup",
            &req.request_id,
        ))
    }

    pub(super) fn modify_cluster_subnet_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ClusterSubnetGroupName").unwrap_or_default();
        let subnet_ids = member_list(req, "SubnetIds", "SubnetIdentifier");
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let g = acct
            .subnet_groups
            .get_mut(&name)
            .ok_or_else(|| subnet_group_not_found(&name))?;
        if let Some(d) = param(req, "Description") {
            g.description = d;
        }
        if !subnet_ids.is_empty() {
            g.subnets = subnet_ids
                .into_iter()
                .map(|id| Subnet {
                    subnet_identifier: id,
                    subnet_availability_zone: format!("{}a", req.region),
                    subnet_status: "Active".to_string(),
                })
                .collect();
        }
        let out = render_subnet_group(g);
        Ok(xml_resp(
            "ModifyClusterSubnetGroup",
            format!("<ClusterSubnetGroup>{out}</ClusterSubnetGroup>"),
            &req.request_id,
        ))
    }

    // ── Security groups ───────────────────────────────────────────
    pub(super) fn create_cluster_security_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ClusterSecurityGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.security_groups.contains_key(&name) {
            return Err(security_group_already_exists(&name));
        }
        let g = ClusterSecurityGroup {
            cluster_security_group_name: name.clone(),
            description: param(req, "Description").unwrap_or_default(),
            ec2_security_groups: Vec::new(),
            ip_ranges: Vec::new(),
            tags: parse_tags(req),
        };
        acct.security_groups.insert(name, g.clone());
        Ok(xml_resp(
            "CreateClusterSecurityGroup",
            format!(
                "<ClusterSecurityGroup>{}</ClusterSecurityGroup>",
                render_security_group(&g)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_security_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let acct = guard.accounts.get(&req.account_id);
        let all: Vec<ClusterSecurityGroup> = match (param(req, "ClusterSecurityGroupName"), acct) {
            (Some(n), Some(a)) => match a.security_groups.get(&n) {
                Some(g) => vec![g.clone()],
                None => return Err(security_group_not_found(&n)),
            },
            (Some(n), None) => return Err(security_group_not_found(&n)),
            (None, Some(a)) => a.security_groups.values().cloned().collect(),
            (None, None) => Vec::new(),
        };
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|g| {
                format!(
                    "<ClusterSecurityGroup>{}</ClusterSecurityGroup>",
                    render_security_group(g)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeClusterSecurityGroups",
            format!(
                "{}<ClusterSecurityGroups>{inner}</ClusterSecurityGroups>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_cluster_security_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ClusterSecurityGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.security_groups.remove(&name).is_none() {
            return Err(security_group_not_found(&name));
        }
        Ok(xml_metadata_only(
            "DeleteClusterSecurityGroup",
            &req.request_id,
        ))
    }

    pub(super) fn authorize_cluster_security_group_ingress(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ClusterSecurityGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let g = acct
            .security_groups
            .get_mut(&name)
            .ok_or_else(|| security_group_not_found(&name))?;
        if let Some(cidr) = param(req, "CIDRIP") {
            g.ip_ranges.push(IpRange {
                status: "authorized".to_string(),
                cidrip: cidr,
            });
        } else if let Some(ec2) = param(req, "EC2SecurityGroupName") {
            g.ec2_security_groups.push(Ec2SecurityGroup {
                status: "authorized".to_string(),
                ec2_security_group_name: ec2,
                ec2_security_group_owner_id: param(req, "EC2SecurityGroupOwnerId"),
            });
        }
        let out = render_security_group(g);
        Ok(xml_resp(
            "AuthorizeClusterSecurityGroupIngress",
            format!("<ClusterSecurityGroup>{out}</ClusterSecurityGroup>"),
            &req.request_id,
        ))
    }

    pub(super) fn revoke_cluster_security_group_ingress(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ClusterSecurityGroupName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let g = acct
            .security_groups
            .get_mut(&name)
            .ok_or_else(|| security_group_not_found(&name))?;
        if let Some(cidr) = param(req, "CIDRIP") {
            g.ip_ranges.retain(|r| r.cidrip != cidr);
        }
        if let Some(ec2) = param(req, "EC2SecurityGroupName") {
            g.ec2_security_groups
                .retain(|e| e.ec2_security_group_name != ec2);
        }
        let out = render_security_group(g);
        Ok(xml_resp(
            "RevokeClusterSecurityGroupIngress",
            format!("<ClusterSecurityGroup>{out}</ClusterSecurityGroup>"),
            &req.request_id,
        ))
    }
}
