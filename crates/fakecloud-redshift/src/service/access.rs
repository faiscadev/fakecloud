//! Endpoint access/authorization, datashares, partners, reserved nodes,
//! integrations, IDC applications, custom domains, authentication profiles,
//! namespaces, and the read-only cluster catalog.

use chrono::Utc;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::helpers::*;
use super::RedshiftService;
use crate::state::{
    AuthenticationProfile, CustomDomainAssociation, DataShare, EndpointAccess, Integration,
    Partner, RedshiftIdcApplication, ReservedNode,
};

fn render_endpoint(e: &EndpointAccess) -> String {
    let sgs: String = e
        .vpc_security_group_ids
        .iter()
        .map(|g| {
            format!(
                "<VpcSecurityGroup><VpcSecurityGroupId>{}</VpcSecurityGroupId><Status>active</Status></VpcSecurityGroup>",
                xml_escape(g)
            )
        })
        .collect();
    // Redshift provisions an interface VPC endpoint (with an ENI per subnet)
    // for every managed endpoint; surface a well-formed one so the Terraform
    // resource's computed `vpc_endpoint` block is populated.
    let token: String = e
        .endpoint_name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(12)
        .collect();
    let vpc_endpoint = format!(
        "<VpcEndpoint><VpcEndpointId>vpce-{token}</VpcEndpointId><VpcId>vpc-fakecloud</VpcId>\
         <NetworkInterfaces><NetworkInterface><NetworkInterfaceId>eni-{token}</NetworkInterfaceId>\
         <SubnetId>subnet-fakecloud</SubnetId><PrivateIpAddress>10.0.0.10</PrivateIpAddress>\
         <AvailabilityZone>us-east-1a</AvailabilityZone></NetworkInterface></NetworkInterfaces></VpcEndpoint>",
    );
    format!(
        "<EndpointName>{name}</EndpointName><ClusterIdentifier>{cluster}</ClusterIdentifier>\
         <SubnetGroupName>{subnet}</SubnetGroupName><EndpointStatus>{status}</EndpointStatus>\
         <EndpointCreateTime>{created}</EndpointCreateTime><ResourceOwner>{owner}</ResourceOwner>\
         <Port>{port}</Port><Address>{addr}</Address>\
         <VpcSecurityGroups>{sgs}</VpcSecurityGroups>{vpc_endpoint}",
        name = xml_escape(&e.endpoint_name),
        cluster = xml_escape(&e.cluster_identifier),
        subnet = xml_escape(&e.subnet_group_name),
        status = xml_escape(&e.endpoint_status),
        created = e.endpoint_create_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        owner = xml_escape(&e.resource_owner),
        port = e.port,
        addr = xml_escape(&e.address),
    )
}

fn render_data_share(d: &DataShare) -> String {
    format!(
        "<DataShareArn>{arn}</DataShareArn><ProducerArn>{producer}</ProducerArn>\
         <AllowPubliclyAccessibleConsumers>{public}</AllowPubliclyAccessibleConsumers>\
         <DataShareType>{dtype}</DataShareType>{managed}<DataShareAssociations/>",
        arn = xml_escape(&d.data_share_arn),
        producer = xml_escape(&d.producer_arn),
        public = d.allow_publicly_accessible_consumers,
        dtype = xml_escape(&d.data_share_type),
        managed = opt_elem("ManagedBy", d.managed_by.as_deref()),
    )
}

fn render_integration(i: &Integration) -> String {
    format!(
        "<IntegrationArn>{arn}</IntegrationArn><IntegrationName>{name}</IntegrationName>\
         <SourceArn>{source}</SourceArn><TargetArn>{target}</TargetArn><Status>{status}</Status>\
         <CreateTime>{created}</CreateTime>{desc}{kms}{tags}",
        arn = xml_escape(&i.integration_arn),
        name = xml_escape(&i.integration_name),
        source = xml_escape(&i.source_arn),
        target = xml_escape(&i.target_arn),
        status = xml_escape(&i.status),
        created = i.create_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        desc = opt_elem("Description", i.description.as_deref()),
        kms = opt_elem("KMSKeyId", i.kms_key_id.as_deref()),
        tags = render_tags(&i.tags),
    )
}

fn render_idc_application(a: &RedshiftIdcApplication) -> String {
    format!(
        "<RedshiftIdcApplicationName>{name}</RedshiftIdcApplicationName>\
         <RedshiftIdcApplicationArn>{arn}</RedshiftIdcApplicationArn>{namespace}\
         <IdcInstanceArn>{instance}</IdcInstanceArn><IdcDisplayName>{display}</IdcDisplayName>\
         <IamRoleArn>{iam}</IamRoleArn><IdcManagedApplicationArn>{managed}</IdcManagedApplicationArn>",
        name = xml_escape(&a.redshift_idc_application_name),
        arn = xml_escape(&a.redshift_idc_application_arn),
        namespace = opt_elem("IdentityNamespace", a.identity_namespace.as_deref()),
        instance = xml_escape(&a.idc_instance_arn),
        display = xml_escape(&a.idc_display_name),
        iam = xml_escape(&a.iam_role_arn),
        managed = xml_escape(&a.idc_managed_application_arn),
    )
}

fn render_custom_domain(c: &CustomDomainAssociation) -> String {
    format!(
        "<CustomDomainName>{name}</CustomDomainName>\
         <CustomDomainCertificateArn>{cert}</CustomDomainCertificateArn>\
         <ClusterIdentifier>{cluster}</ClusterIdentifier>\
         <CustomDomainCertExpiryTime>{expiry}</CustomDomainCertExpiryTime>",
        name = xml_escape(&c.custom_domain_name),
        cert = xml_escape(&c.custom_domain_certificate_arn),
        cluster = xml_escape(&c.cluster_identifier),
        expiry = c
            .custom_domain_certificate_expiry_date
            .format("%Y-%m-%dT%H:%M:%S%.3fZ"),
    )
}

fn render_partner(p: &Partner) -> String {
    format!(
        "<DatabaseName>{db}</DatabaseName><PartnerName>{partner}</PartnerName>\
         <ClusterIdentifier>{cluster}</ClusterIdentifier><Status>{status}</Status>{msg}",
        db = xml_escape(&p.database_name),
        partner = xml_escape(&p.partner_name),
        cluster = xml_escape(&p.cluster_identifier),
        status = xml_escape(&p.status),
        msg = opt_elem("StatusMessage", p.status_message.as_deref()),
    )
}

fn render_reserved_node(r: &ReservedNode) -> String {
    format!(
        "<ReservedNodeId>{id}</ReservedNodeId><ReservedNodeOfferingId>{offering}</ReservedNodeOfferingId>\
         <NodeType>{node_type}</NodeType><StartTime>{start}</StartTime><Duration>{duration}</Duration>\
         <FixedPrice>{fixed}</FixedPrice><UsagePrice>{usage}</UsagePrice><CurrencyCode>{currency}</CurrencyCode>\
         <NodeCount>{count}</NodeCount><State>{state}</State><OfferingType>{otype}</OfferingType>\
         <ReservedNodeOfferingType>{rotype}</ReservedNodeOfferingType>",
        id = xml_escape(&r.reserved_node_id),
        offering = xml_escape(&r.reserved_node_offering_id),
        node_type = xml_escape(&r.node_type),
        start = r.start_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        duration = r.duration,
        fixed = r.fixed_price,
        usage = r.usage_price,
        currency = xml_escape(&r.currency_code),
        count = r.node_count,
        state = xml_escape(&r.state),
        otype = xml_escape(&r.offering_type),
        rotype = xml_escape(&r.reserved_node_offering_type),
    )
}

impl RedshiftService {
    // ── Endpoint access ───────────────────────────────────────────
    pub(super) fn create_endpoint_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "EndpointName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.endpoint_access.contains_key(&name) {
            return Err(endpoint_already_exists(&name));
        }
        let token = &Uuid::new_v4().simple().to_string()[..12];
        let cluster_id = param(req, "ClusterIdentifier").unwrap_or_default();
        // When the caller supplies no security groups, Redshift attaches the
        // cluster's effective VPC security groups (the VPC default group when
        // the cluster itself has none), so the endpoint always reports at least
        // one — mirror that so the resource's `vpc_security_group_ids` is set.
        let mut vpc_security_group_ids =
            member_list(req, "VpcSecurityGroupIds", "VpcSecurityGroupId");
        if vpc_security_group_ids.is_empty() {
            vpc_security_group_ids = acct
                .clusters
                .get(&cluster_id)
                .map(|c| c.vpc_security_group_ids.clone())
                .unwrap_or_default();
            if vpc_security_group_ids.is_empty() {
                vpc_security_group_ids = vec!["sg-fakecloud0default".to_string()];
            }
        }
        let e = EndpointAccess {
            endpoint_name: name.clone(),
            cluster_identifier: cluster_id,
            subnet_group_name: param(req, "SubnetGroupName").unwrap_or_default(),
            endpoint_status: "active".to_string(),
            endpoint_create_time: Utc::now(),
            resource_owner: param(req, "ResourceOwner").unwrap_or_else(|| req.account_id.clone()),
            port: 5439,
            address: format!("{name}-{token}.{}.redshift.amazonaws.com", req.region),
            vpc_security_group_ids,
        };
        acct.endpoint_access.insert(name, e.clone());
        Ok(xml_resp(
            "CreateEndpointAccess",
            render_endpoint(&e),
            &req.request_id,
        ))
    }

    pub(super) fn describe_endpoint_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = param(req, "EndpointName");
        let cluster_filter = param(req, "ClusterIdentifier");
        let guard = self.state.read();
        let all: Vec<EndpointAccess> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.endpoint_access
                    .values()
                    .filter(|e| {
                        name_filter
                            .as_ref()
                            .map(|n| &e.endpoint_name == n)
                            .unwrap_or(true)
                            && cluster_filter
                                .as_ref()
                                .map(|c| &e.cluster_identifier == c)
                                .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|e| format!("<member>{}</member>", render_endpoint(e)))
            .collect();
        Ok(xml_resp(
            "DescribeEndpointAccess",
            format!(
                "{}<EndpointAccessList>{inner}</EndpointAccessList>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn modify_endpoint_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "EndpointName").unwrap_or_default();
        let sgs = member_list(req, "VpcSecurityGroupIds", "VpcSecurityGroupId");
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let e = acct
            .endpoint_access
            .get_mut(&name)
            .ok_or_else(|| endpoint_not_found(&name))?;
        if !sgs.is_empty() {
            e.vpc_security_group_ids = sgs;
        }
        Ok(xml_resp(
            "ModifyEndpointAccess",
            render_endpoint(e),
            &req.request_id,
        ))
    }

    pub(super) fn delete_endpoint_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "EndpointName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let e = acct
            .endpoint_access
            .remove(&name)
            .ok_or_else(|| endpoint_not_found(&name))?;
        Ok(xml_resp(
            "DeleteEndpointAccess",
            render_endpoint(&e),
            &req.request_id,
        ))
    }

    pub(super) fn authorize_endpoint_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.endpoint_authorization(req, "AuthorizeEndpointAccess", "Authorized")
    }

    pub(super) fn revoke_endpoint_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.endpoint_authorization(req, "RevokeEndpointAccess", "Revoking")
    }

    fn endpoint_authorization(
        &self,
        req: &AwsRequest,
        action: &str,
        status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let grantee = param(req, "Account").unwrap_or_default();
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let vpcs = member_list(req, "VpcIds", "VpcIdentifier");
        let allowed_all = vpcs.is_empty();
        let vpc_xml: String = vpcs.iter().map(|v| tag_elem("VpcIdentifier", v)).collect();
        Ok(xml_resp(
            action,
            format!(
                "<Grantor>{grantor}</Grantor><Grantee>{grantee}</Grantee>\
                 <ClusterIdentifier>{cluster}</ClusterIdentifier><AuthorizeTime>{now}</AuthorizeTime>\
                 <ClusterStatus>available</ClusterStatus><Status>{status}</Status>\
                 <AllowedAllVPCs>{allowed_all}</AllowedAllVPCs><AllowedVPCs>{vpc_xml}</AllowedVPCs>\
                 <EndpointCount>0</EndpointCount>",
                grantor = xml_escape(&req.account_id),
                grantee = xml_escape(&grantee),
                cluster = xml_escape(&cluster),
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_endpoint_authorization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeEndpointAuthorization",
            "<EndpointAuthorizationList/>".to_string(),
            &req.request_id,
        ))
    }

    // ── Datashares ────────────────────────────────────────────────
    fn upsert_datashare(
        &self,
        req: &AwsRequest,
        status: &str,
        managed_by: Option<String>,
    ) -> DataShare {
        let arn = param(req, "DataShareArn").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let entry = acct
            .datashares
            .entry(arn.clone())
            .or_insert_with(|| DataShare {
                data_share_arn: arn.clone(),
                producer_arn: arn.clone(),
                allow_publicly_accessible_consumers: false,
                managed_by: None,
                data_share_type: "INTERNAL".to_string(),
                status: status.to_string(),
            });
        entry.status = status.to_string();
        if managed_by.is_some() {
            entry.managed_by = managed_by;
        }
        entry.clone()
    }

    pub(super) fn associate_data_share_consumer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = self.upsert_datashare(req, "ACTIVE", None);
        Ok(xml_resp(
            "AssociateDataShareConsumer",
            render_data_share(&d),
            &req.request_id,
        ))
    }

    pub(super) fn disassociate_data_share_consumer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = self.upsert_datashare(req, "AVAILABLE", None);
        Ok(xml_resp(
            "DisassociateDataShareConsumer",
            render_data_share(&d),
            &req.request_id,
        ))
    }

    pub(super) fn authorize_data_share(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = self.upsert_datashare(req, "AUTHORIZED", None);
        Ok(xml_resp(
            "AuthorizeDataShare",
            render_data_share(&d),
            &req.request_id,
        ))
    }

    pub(super) fn deauthorize_data_share(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = self.upsert_datashare(req, "DEAUTHORIZED", None);
        Ok(xml_resp(
            "DeauthorizeDataShare",
            render_data_share(&d),
            &req.request_id,
        ))
    }

    pub(super) fn reject_data_share(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let d = self.upsert_datashare(req, "REJECTED", None);
        Ok(xml_resp(
            "RejectDataShare",
            render_data_share(&d),
            &req.request_id,
        ))
    }

    pub(super) fn describe_data_shares(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        if let Some(arn) = param(req, "DataShareArn") {
            let guard = self.state.read();
            let d = guard
                .accounts
                .get(&req.account_id)
                .and_then(|a| a.datashares.get(&arn))
                .cloned()
                .ok_or_else(|| invalid_datashare(format!("DataShare {arn} not found.")))?;
            return Ok(xml_resp(
                "DescribeDataShares",
                format!(
                    "<DataShares><member>{}</member></DataShares>",
                    render_data_share(&d)
                ),
                &req.request_id,
            ));
        }
        let guard = self.state.read();
        let all: Vec<DataShare> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.datashares.values().cloned().collect())
            .unwrap_or_default();
        let inner: String = all
            .iter()
            .map(|d| format!("<member>{}</member>", render_data_share(d)))
            .collect();
        Ok(xml_resp(
            "DescribeDataShares",
            format!("<DataShares>{inner}</DataShares>"),
            &req.request_id,
        ))
    }

    pub(super) fn describe_data_shares_for_consumer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeDataSharesForConsumer",
            "<DataShares/>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_data_shares_for_producer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeDataSharesForProducer",
            "<DataShares/>".to_string(),
            &req.request_id,
        ))
    }

    // ── Partners ──────────────────────────────────────────────────
    fn partner_key(cluster: &str, db: &str, partner: &str) -> String {
        format!("{cluster}\u{1}{db}\u{1}{partner}")
    }

    pub(super) fn add_partner(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let db = param(req, "DatabaseName").unwrap_or_default();
        let partner = param(req, "PartnerName").unwrap_or_default();
        let key = Self::partner_key(&cluster, &db, &partner);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        acct.partners.insert(
            key,
            Partner {
                database_name: db.clone(),
                partner_name: partner.clone(),
                cluster_identifier: cluster,
                account_id: param(req, "AccountId").unwrap_or_default(),
                status: "Active".to_string(),
                status_message: None,
            },
        );
        Ok(xml_resp(
            "AddPartner",
            format!(
                "<DatabaseName>{}</DatabaseName><PartnerName>{}</PartnerName>",
                xml_escape(&db),
                xml_escape(&partner)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_partners(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let db_filter = param(req, "DatabaseName");
        let partner_filter = param(req, "PartnerName");
        let guard = self.state.read();
        let items: Vec<Partner> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.partners
                    .values()
                    .filter(|p| {
                        p.cluster_identifier == cluster
                            && db_filter
                                .as_ref()
                                .map(|d| &p.database_name == d)
                                .unwrap_or(true)
                            && partner_filter
                                .as_ref()
                                .map(|n| &p.partner_name == n)
                                .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let inner: String = items
            .iter()
            .map(|p| {
                format!(
                    "<PartnerIntegrationInfo>{}</PartnerIntegrationInfo>",
                    render_partner(p)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribePartners",
            format!("<PartnerIntegrationInfoList>{inner}</PartnerIntegrationInfoList>"),
            &req.request_id,
        ))
    }

    pub(super) fn delete_partner(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let db = param(req, "DatabaseName").unwrap_or_default();
        let partner = param(req, "PartnerName").unwrap_or_default();
        let key = Self::partner_key(&cluster, &db, &partner);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        acct.partners.remove(&key);
        Ok(xml_resp(
            "DeletePartner",
            format!(
                "<DatabaseName>{}</DatabaseName><PartnerName>{}</PartnerName>",
                xml_escape(&db),
                xml_escape(&partner)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn update_partner_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let db = param(req, "DatabaseName").unwrap_or_default();
        let partner = param(req, "PartnerName").unwrap_or_default();
        let key = Self::partner_key(&cluster, &db, &partner);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if let Some(p) = acct.partners.get_mut(&key) {
            if let Some(s) = param(req, "Status") {
                p.status = s;
            }
            p.status_message = param(req, "StatusMessage");
        }
        Ok(xml_resp(
            "UpdatePartnerStatus",
            format!(
                "<DatabaseName>{}</DatabaseName><PartnerName>{}</PartnerName>",
                xml_escape(&db),
                xml_escape(&partner)
            ),
            &req.request_id,
        ))
    }

    // ── Reserved nodes ────────────────────────────────────────────
    pub(super) fn describe_reserved_nodes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let all: Vec<ReservedNode> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.reserved_nodes.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|r| format!("<ReservedNode>{}</ReservedNode>", render_reserved_node(r)))
            .collect();
        Ok(xml_resp(
            "DescribeReservedNodes",
            format!(
                "{}<ReservedNodes>{inner}</ReservedNodes>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_reserved_node_offerings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeReservedNodeOfferings",
            format!(
                "<ReservedNodeOfferings>{}</ReservedNodeOfferings>",
                default_offering_xml()
            ),
            &req.request_id,
        ))
    }

    pub(super) fn purchase_reserved_node_offering(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let offering = param(req, "ReservedNodeOfferingId").unwrap_or_default();
        let id = format!("rn-{}", &Uuid::new_v4().simple().to_string()[..12]);
        let node = ReservedNode {
            reserved_node_id: id.clone(),
            reserved_node_offering_id: offering,
            node_type: "ra3.xlplus".to_string(),
            start_time: Utc::now(),
            duration: 31536000,
            fixed_price: 0.0,
            usage_price: 0.0,
            currency_code: "USD".to_string(),
            node_count: int_param(req, "NodeCount").unwrap_or(1),
            state: "payment-pending".to_string(),
            offering_type: "No Upfront".to_string(),
            reserved_node_offering_type: "Regular".to_string(),
        };
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        acct.reserved_nodes.insert(id, node.clone());
        Ok(xml_resp(
            "PurchaseReservedNodeOffering",
            format!(
                "<ReservedNode>{}</ReservedNode>",
                render_reserved_node(&node)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn accept_reserved_node_exchange(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let src = param(req, "ReservedNodeId").unwrap_or_default();
        let guard = self.state.read();
        let node = guard
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.reserved_nodes.get(&src))
            .cloned();
        let node_xml = node
            .map(|n| render_reserved_node(&n))
            .unwrap_or_else(|| {
                let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
                format!(
                    "<ReservedNodeId>{}</ReservedNodeId><ReservedNodeOfferingId>{}</ReservedNodeOfferingId><NodeType>ra3.xlplus</NodeType><StartTime>{now}</StartTime><Duration>31536000</Duration><State>active</State><NodeCount>1</NodeCount>",
                    xml_escape(&src),
                    xml_escape(&param(req, "TargetReservedNodeOfferingId").unwrap_or_default()),
                )
            });
        Ok(xml_resp(
            "AcceptReservedNodeExchange",
            format!("<ExchangedReservedNode>{node_xml}</ExchangedReservedNode>"),
            &req.request_id,
        ))
    }

    pub(super) fn get_reserved_node_exchange_offerings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "GetReservedNodeExchangeOfferings",
            format!(
                "<ReservedNodeOfferings>{}</ReservedNodeOfferings>",
                default_offering_xml()
            ),
            &req.request_id,
        ))
    }

    pub(super) fn get_reserved_node_exchange_configuration_options(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "GetReservedNodeExchangeConfigurationOptions",
            "<ReservedNodeConfigurationOptionList/>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_reserved_node_exchange_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeReservedNodeExchangeStatus",
            "<ReservedNodeExchangeStatusDetails/>".to_string(),
            &req.request_id,
        ))
    }

    // ── Integrations ──────────────────────────────────────────────
    pub(super) fn create_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "IntegrationName").unwrap_or_default();
        let arn = Arn::new(
            "redshift",
            &req.region,
            &req.account_id,
            &format!("integration:{}", &Uuid::new_v4().simple().to_string()[..12]),
        )
        .to_string();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct
            .integrations
            .values()
            .any(|i| i.integration_name == name)
        {
            return Err(integration_already_exists(&name));
        }
        let integ = Integration {
            integration_arn: arn.clone(),
            integration_name: name,
            source_arn: param(req, "SourceArn").unwrap_or_default(),
            target_arn: param(req, "TargetArn").unwrap_or_default(),
            status: "creating".to_string(),
            create_time: Utc::now(),
            description: param(req, "Description"),
            kms_key_id: param(req, "KMSKeyId"),
            tags: parse_tags(req),
        };
        acct.integrations.insert(arn, integ.clone());
        Ok(xml_resp(
            "CreateIntegration",
            render_integration(&integ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_integrations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let acct = guard.accounts.get(&req.account_id);
        let all: Vec<Integration> = match (param(req, "IntegrationArn"), acct) {
            (Some(arn), Some(a)) => match a.integrations.get(&arn) {
                Some(i) => vec![i.clone()],
                None => return Err(integration_not_found(&arn)),
            },
            (Some(arn), None) => return Err(integration_not_found(&arn)),
            (None, Some(a)) => a.integrations.values().cloned().collect(),
            (None, None) => Vec::new(),
        };
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|i| format!("<Integration>{}</Integration>", render_integration(i)))
            .collect();
        Ok(xml_resp(
            "DescribeIntegrations",
            format!(
                "{}<Integrations>{inner}</Integrations>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn modify_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "IntegrationArn").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let i = acct
            .integrations
            .get_mut(&arn)
            .ok_or_else(|| integration_not_found(&arn))?;
        if let Some(n) = param(req, "IntegrationName") {
            i.integration_name = n;
        }
        if let Some(d) = param(req, "Description") {
            i.description = Some(d);
        }
        Ok(xml_resp(
            "ModifyIntegration",
            render_integration(i),
            &req.request_id,
        ))
    }

    pub(super) fn delete_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "IntegrationArn").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let i = acct
            .integrations
            .remove(&arn)
            .ok_or_else(|| integration_not_found(&arn))?;
        Ok(xml_resp(
            "DeleteIntegration",
            render_integration(&i),
            &req.request_id,
        ))
    }

    pub(super) fn describe_inbound_integrations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeInboundIntegrations",
            "<InboundIntegrations/>".to_string(),
            &req.request_id,
        ))
    }

    // ── Redshift IDC applications ─────────────────────────────────
    pub(super) fn create_redshift_idc_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "RedshiftIdcApplicationName").unwrap_or_default();
        let arn = Arn::new(
            "redshift",
            &req.region,
            &req.account_id,
            &format!(
                "redshiftidcapplication:{}",
                &Uuid::new_v4().simple().to_string()[..12]
            ),
        )
        .to_string();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct
            .idc_applications
            .values()
            .any(|a| a.redshift_idc_application_name == name)
        {
            return Err(idc_application_already_exists(&name));
        }
        let app = RedshiftIdcApplication {
            redshift_idc_application_name: name,
            redshift_idc_application_arn: arn.clone(),
            identity_namespace: param(req, "IdentityNamespace"),
            idc_instance_arn: param(req, "IdcInstanceArn").unwrap_or_default(),
            idc_display_name: param(req, "IdcDisplayName").unwrap_or_default(),
            iam_role_arn: param(req, "IamRoleArn").unwrap_or_default(),
            idc_managed_application_arn: format!(
                "arn:aws:sso::{}:application/fakecloud",
                req.account_id
            ),
            authorized_token_issuer_list: Vec::new(),
            service_integrations: Vec::new(),
        };
        acct.idc_applications.insert(arn, app.clone());
        Ok(xml_resp(
            "CreateRedshiftIdcApplication",
            format!(
                "<RedshiftIdcApplication>{}</RedshiftIdcApplication>",
                render_idc_application(&app)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_redshift_idc_applications(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let all: Vec<RedshiftIdcApplication> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| {
                let filter = param(req, "RedshiftIdcApplicationArn");
                a.idc_applications
                    .values()
                    .filter(|app| {
                        filter
                            .as_ref()
                            .map(|f| &app.redshift_idc_application_arn == f)
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|a| format!("<member>{}</member>", render_idc_application(a)))
            .collect();
        Ok(xml_resp(
            "DescribeRedshiftIdcApplications",
            format!(
                "{}<RedshiftIdcApplications>{inner}</RedshiftIdcApplications>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn modify_redshift_idc_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "RedshiftIdcApplicationArn").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let app = acct
            .idc_applications
            .get_mut(&arn)
            .ok_or_else(|| idc_application_not_found(&arn))?;
        if let Some(v) = param(req, "IdcDisplayName") {
            app.idc_display_name = v;
        }
        if let Some(v) = param(req, "IamRoleArn") {
            app.iam_role_arn = v;
        }
        if let Some(v) = param(req, "IdentityNamespace") {
            app.identity_namespace = Some(v);
        }
        Ok(xml_resp(
            "ModifyRedshiftIdcApplication",
            format!(
                "<RedshiftIdcApplication>{}</RedshiftIdcApplication>",
                render_idc_application(app)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_redshift_idc_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "RedshiftIdcApplicationArn").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.idc_applications.remove(&arn).is_none() {
            return Err(idc_application_not_found(&arn));
        }
        Ok(xml_metadata_only(
            "DeleteRedshiftIdcApplication",
            &req.request_id,
        ))
    }

    // ── Custom domain associations ────────────────────────────────
    fn custom_domain_key(cluster: &str, domain: &str) -> String {
        format!("{cluster}\u{1}{domain}")
    }

    pub(super) fn create_custom_domain_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let domain = param(req, "CustomDomainName").unwrap_or_default();
        let assoc = CustomDomainAssociation {
            custom_domain_name: domain.clone(),
            custom_domain_certificate_arn: param(req, "CustomDomainCertificateArn")
                .unwrap_or_default(),
            cluster_identifier: cluster.clone(),
            custom_domain_certificate_expiry_date: Utc::now() + chrono::Duration::days(365),
        };
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        acct.custom_domains
            .insert(Self::custom_domain_key(&cluster, &domain), assoc.clone());
        Ok(xml_resp(
            "CreateCustomDomainAssociation",
            render_custom_domain(&assoc),
            &req.request_id,
        ))
    }

    pub(super) fn describe_custom_domain_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain_filter = param(req, "CustomDomainName");
        let guard = self.state.read();
        let all: Vec<CustomDomainAssociation> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.custom_domains
                    .values()
                    .filter(|c| {
                        domain_filter
                            .as_ref()
                            .map(|d| &c.custom_domain_name == d)
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let inner: String = all
            .iter()
            .map(|c| {
                // The `Associations` (`AssociationList`) member serializes each
                // element under its `xmlName` wrapper `Association` — not
                // `CustomDomainAssociation`. Using the wrong wrapper makes the
                // AWS SDK / Terraform provider parse an empty association list.
                format!(
                    "<Association><CustomDomainCertificateArn>{cert}</CustomDomainCertificateArn><CustomDomainCertificateExpiryDate>{expiry}</CustomDomainCertificateExpiryDate><CertificateAssociations><CertificateAssociation><CustomDomainName>{name}</CustomDomainName><ClusterIdentifier>{cluster}</ClusterIdentifier></CertificateAssociation></CertificateAssociations></Association>",
                    cert = xml_escape(&c.custom_domain_certificate_arn),
                    expiry = c.custom_domain_certificate_expiry_date.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    name = xml_escape(&c.custom_domain_name),
                    cluster = xml_escape(&c.cluster_identifier),
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeCustomDomainAssociations",
            format!("<Associations>{inner}</Associations>"),
            &req.request_id,
        ))
    }

    pub(super) fn modify_custom_domain_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let domain = param(req, "CustomDomainName").unwrap_or_default();
        let key = Self::custom_domain_key(&cluster, &domain);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let assoc = acct
            .custom_domains
            .entry(key)
            .or_insert_with(|| CustomDomainAssociation {
                custom_domain_name: domain.clone(),
                custom_domain_certificate_arn: String::new(),
                cluster_identifier: cluster.clone(),
                custom_domain_certificate_expiry_date: Utc::now() + chrono::Duration::days(365),
            });
        if let Some(arn) = param(req, "CustomDomainCertificateArn") {
            assoc.custom_domain_certificate_arn = arn;
        }
        Ok(xml_resp(
            "ModifyCustomDomainAssociation",
            render_custom_domain(assoc),
            &req.request_id,
        ))
    }

    pub(super) fn delete_custom_domain_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let domain = param(req, "CustomDomainName").unwrap_or_default();
        let key = Self::custom_domain_key(&cluster, &domain);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.custom_domains.remove(&key).is_none() {
            return Err(custom_domain_not_found(&domain));
        }
        Ok(xml_metadata_only(
            "DeleteCustomDomainAssociation",
            &req.request_id,
        ))
    }

    // ── Authentication profiles ───────────────────────────────────
    pub(super) fn create_authentication_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "AuthenticationProfileName").unwrap_or_default();
        let content = param(req, "AuthenticationProfileContent").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.authentication_profiles.contains_key(&name) {
            return Err(authentication_profile_already_exists(&name));
        }
        acct.authentication_profiles.insert(
            name.clone(),
            AuthenticationProfile {
                authentication_profile_name: name.clone(),
                authentication_profile_content: content.clone(),
            },
        );
        Ok(xml_resp(
            "CreateAuthenticationProfile",
            format!(
                "<AuthenticationProfileName>{}</AuthenticationProfileName><AuthenticationProfileContent>{}</AuthenticationProfileContent>",
                xml_escape(&name),
                xml_escape(&content)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_authentication_profiles(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = param(req, "AuthenticationProfileName");
        let guard = self.state.read();
        let all: Vec<AuthenticationProfile> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.authentication_profiles
                    .values()
                    .filter(|p| {
                        name_filter
                            .as_ref()
                            .map(|n| &p.authentication_profile_name == n)
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let inner: String = all
            .iter()
            .map(|p| {
                format!(
                    "<member><AuthenticationProfileName>{}</AuthenticationProfileName><AuthenticationProfileContent>{}</AuthenticationProfileContent></member>",
                    xml_escape(&p.authentication_profile_name),
                    xml_escape(&p.authentication_profile_content)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeAuthenticationProfiles",
            format!("<AuthenticationProfiles>{inner}</AuthenticationProfiles>"),
            &req.request_id,
        ))
    }

    pub(super) fn modify_authentication_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "AuthenticationProfileName").unwrap_or_default();
        let content = param(req, "AuthenticationProfileContent").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let p = acct
            .authentication_profiles
            .get_mut(&name)
            .ok_or_else(|| authentication_profile_not_found(&name))?;
        p.authentication_profile_content = content.clone();
        Ok(xml_resp(
            "ModifyAuthenticationProfile",
            format!(
                "<AuthenticationProfileName>{}</AuthenticationProfileName><AuthenticationProfileContent>{}</AuthenticationProfileContent>",
                xml_escape(&name),
                xml_escape(&content)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_authentication_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "AuthenticationProfileName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.authentication_profiles.remove(&name).is_none() {
            return Err(authentication_profile_not_found(&name));
        }
        Ok(xml_resp(
            "DeleteAuthenticationProfile",
            format!(
                "<AuthenticationProfileName>{}</AuthenticationProfileName>",
                xml_escape(&name)
            ),
            &req.request_id,
        ))
    }

    // ── Namespaces / identity center ──────────────────────────────
    pub(super) fn register_namespace(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ns = param(req, "NamespaceIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        acct.registered_namespaces
            .insert(ns, "Registered".to_string());
        Ok(xml_resp(
            "RegisterNamespace",
            "<Status>Registering</Status>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn deregister_namespace(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ns = param(req, "NamespaceIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        acct.registered_namespaces.remove(&ns);
        Ok(xml_resp(
            "DeregisterNamespace",
            "<Status>Deregistering</Status>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn get_identity_center_auth_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "GetIdentityCenterAuthToken",
            format!(
                "<IdentityCenterAuthToken>{}</IdentityCenterAuthToken>",
                xml_escape(&format!("fc-idc-{}", Uuid::new_v4().simple()))
            ),
            &req.request_id,
        ))
    }

    // ── Read-only catalog ─────────────────────────────────────────
    pub(super) fn describe_account_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeAccountAttributes",
            "<AccountAttributes><AccountAttribute><AttributeName>max-defer-maintenance-duration</AttributeName><AttributeValues><AttributeValueTarget><AttributeValue>45</AttributeValue></AttributeValueTarget></AttributeValues></AccountAttribute></AccountAttributes>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_tracks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeClusterTracks",
            "<MaintenanceTracks><MaintenanceTrack><MaintenanceTrackName>current</MaintenanceTrackName><DatabaseVersion>1.0</DatabaseVersion><UpdateTargets/></MaintenanceTrack><MaintenanceTrack><MaintenanceTrackName>trailing</MaintenanceTrackName><DatabaseVersion>1.0</DatabaseVersion><UpdateTargets/></MaintenanceTrack></MaintenanceTracks>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeClusterVersions",
            "<ClusterVersions><ClusterVersion><ClusterVersion>1.0</ClusterVersion><ClusterParameterGroupFamily>redshift-1.0</ClusterParameterGroupFamily><Description>Redshift 1.0</Description></ClusterVersion></ClusterVersions>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_node_configuration_options(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeNodeConfigurationOptions",
            "<NodeConfigurationOptionList><NodeConfigurationOption><NodeType>ra3.xlplus</NodeType><NumberOfNodes>2</NumberOfNodes><EstimatedDiskUtilizationPercent>10.0</EstimatedDiskUtilizationPercent><Mode>standard</Mode></NodeConfigurationOption></NodeConfigurationOptionList>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_orderable_cluster_options(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeOrderableClusterOptions",
            "<OrderableClusterOptions><OrderableClusterOption><ClusterVersion>1.0</ClusterVersion><ClusterType>multi-node</ClusterType><NodeType>ra3.xlplus</NodeType><AvailabilityZones><AvailabilityZone><Name>us-east-1a</Name></AvailabilityZone></AvailabilityZones></OrderableClusterOption></OrderableClusterOptions>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_storage(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "DescribeStorage",
            "<TotalBackupSizeInMegaBytes>0.0</TotalBackupSizeInMegaBytes><TotalProvisionedStorageInMegaBytes>0.0</TotalProvisionedStorageInMegaBytes>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn list_recommendations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(xml_resp(
            "ListRecommendations",
            "<Recommendations/>".to_string(),
            &req.request_id,
        ))
    }
}

fn default_offering_xml() -> String {
    let id = "fake-offering-0001";
    format!(
        "<ReservedNodeOffering><ReservedNodeOfferingId>{id}</ReservedNodeOfferingId><NodeType>ra3.xlplus</NodeType><Duration>31536000</Duration><FixedPrice>0.0</FixedPrice><UsagePrice>0.0</UsagePrice><CurrencyCode>USD</CurrencyCode><OfferingType>No Upfront</OfferingType><RecurringCharges/><ReservedNodeOfferingType>Regular</ReservedNodeOfferingType></ReservedNodeOffering>"
    )
}
