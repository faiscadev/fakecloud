//! XML fragment renderers for Neptune Query-protocol responses.
//!
//! Every renderer returns the inner XML for the named element so the
//! action handler can wrap it in the standard
//! `<ActionResponse><ActionResult>…` envelope via
//! [`fakecloud_core::query::query_response_xml`].

use fakecloud_aws::xml::xml_escape;

use crate::state::{
    DbCluster, DbClusterEndpoint, DbClusterParameterGroup, DbClusterSnapshot, DbInstance,
    DbParameterGroup, DbSubnetGroup, EventSubscription, GlobalCluster, ParameterValue, Tag,
};

/// ISO-8601 timestamp in the form AWS emits for RDS/Neptune `TStamp` fields.
pub(crate) fn ts(t: &chrono::DateTime<chrono::Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn tag(t: &Tag) -> String {
    format!(
        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
        xml_escape(&t.key),
        xml_escape(&t.value)
    )
}

/// Render a `<TagList>` element (used by ListTagsForResource and nested in
/// resource descriptions where applicable).
pub(crate) fn tag_list(tags: &[Tag]) -> String {
    let inner: String = tags.iter().map(tag).collect();
    format!("<TagList>{inner}</TagList>")
}

fn string_list(name: &str, member: &str, items: &[String]) -> String {
    let inner: String = items
        .iter()
        .map(|v| format!("<{member}>{}</{member}>", xml_escape(v)))
        .collect();
    format!("<{name}>{inner}</{name}>")
}

pub(crate) fn db_cluster(c: &DbCluster) -> String {
    let azs = string_list(
        "AvailabilityZones",
        "AvailabilityZone",
        &c.availability_zones,
    );
    let vpc_sgs: String = c
        .vpc_security_group_ids
        .iter()
        .map(|id| {
            format!(
                "<VpcSecurityGroupMembership><VpcSecurityGroupId>{}</VpcSecurityGroupId><Status>active</Status></VpcSecurityGroupMembership>",
                xml_escape(id)
            )
        })
        .collect();
    let logs = string_list(
        "EnabledCloudwatchLogsExports",
        "member",
        &c.enabled_cloudwatch_logs_exports,
    );
    let members: String = c
        .members
        .iter()
        .map(|m| {
            format!(
                "<DBClusterMember><DBInstanceIdentifier>{}</DBInstanceIdentifier><IsClusterWriter>{}</IsClusterWriter><DBClusterParameterGroupStatus>in-sync</DBClusterParameterGroupStatus><PromotionTier>{}</PromotionTier></DBClusterMember>",
                xml_escape(&m.db_instance_identifier),
                m.is_writer,
                m.promotion_tier
            )
        })
        .collect();
    let roles: String = c
        .associated_roles
        .iter()
        .map(|r| {
            let feature = r
                .feature_name
                .as_ref()
                .map(|f| format!("<FeatureName>{}</FeatureName>", xml_escape(f)))
                .unwrap_or_default();
            format!(
                "<DBClusterRole><RoleArn>{}</RoleArn><Status>{}</Status>{feature}</DBClusterRole>",
                xml_escape(&r.role_arn),
                xml_escape(&r.status),
            )
        })
        .collect();
    let kms = c
        .kms_key_id
        .as_ref()
        .map(|k| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(k)))
        .unwrap_or_default();
    format!(
        "<DBClusterIdentifier>{id}</DBClusterIdentifier>\
         <DBClusterArn>{arn}</DBClusterArn>\
         <DbClusterResourceId>{rid}</DbClusterResourceId>\
         <Status>{status}</Status>\
         <Engine>{engine}</Engine>\
         <EngineVersion>{ev}</EngineVersion>\
         <Port>{port}</Port>\
         <MasterUsername>{user}</MasterUsername>\
         <Endpoint>{endpoint}</Endpoint>\
         <ReaderEndpoint>{reader}</ReaderEndpoint>\
         <HostedZoneId>{hz}</HostedZoneId>\
         <MultiAZ>false</MultiAZ>\
         <DBSubnetGroup>{subnet}</DBSubnetGroup>\
         <DBClusterParameterGroup>{pg}</DBClusterParameterGroup>\
         <StorageEncrypted>{enc}</StorageEncrypted>\
         {kms}\
         <DeletionProtection>{del}</DeletionProtection>\
         <IAMDatabaseAuthenticationEnabled>{iam}</IAMDatabaseAuthenticationEnabled>\
         <BackupRetentionPeriod>{brp}</BackupRetentionPeriod>\
         <PreferredBackupWindow>{pbw}</PreferredBackupWindow>\
         <PreferredMaintenanceWindow>{pmw}</PreferredMaintenanceWindow>\
         <StorageType>{st}</StorageType>\
         <ClusterCreateTime>{cct}</ClusterCreateTime>\
         {azs}\
         <VpcSecurityGroups>{vpc_sgs}</VpcSecurityGroups>\
         {logs}\
         <DBClusterMembers>{members}</DBClusterMembers>\
         <AssociatedRoles>{roles}</AssociatedRoles>\
         <ReadReplicaIdentifiers/>",
        id = xml_escape(&c.db_cluster_identifier),
        arn = xml_escape(&c.db_cluster_arn),
        rid = xml_escape(&c.db_cluster_resource_id),
        status = xml_escape(&c.status),
        engine = xml_escape(&c.engine),
        ev = xml_escape(&c.engine_version),
        port = c.port,
        user = xml_escape(&c.master_username),
        endpoint = xml_escape(&c.endpoint),
        reader = xml_escape(&c.reader_endpoint),
        hz = xml_escape(&c.hosted_zone_id),
        subnet = xml_escape(&c.db_subnet_group),
        pg = xml_escape(&c.db_cluster_parameter_group),
        enc = c.storage_encrypted,
        del = c.deletion_protection,
        iam = c.iam_database_authentication_enabled,
        brp = c.backup_retention_period,
        pbw = xml_escape(&c.preferred_backup_window),
        pmw = xml_escape(&c.preferred_maintenance_window),
        st = xml_escape(&c.storage_type),
        cct = ts(&c.cluster_create_time),
    )
}

pub(crate) fn db_cluster_endpoint(e: &DbClusterEndpoint) -> String {
    let static_members = string_list("StaticMembers", "member", &e.static_members);
    let excluded_members = string_list("ExcludedMembers", "member", &e.excluded_members);
    // The optional members are omitted when empty rather than rendered
    // blank. A cluster's built-in writer and reader endpoints carry no
    // identifier, resource id, custom type or ARN, and an empty element
    // reads as a resource named "" -- an identifier a client might try
    // to delete, or an ARN it might try to resolve.
    let optional = |tag: &str, value: &str| -> String {
        if value.is_empty() {
            String::new()
        } else {
            format!("<{tag}>{}</{tag}>", xml_escape(value))
        }
    };
    format!(
        "{id}\
         <DBClusterIdentifier>{cluster}</DBClusterIdentifier>\
         {rid}\
         <Endpoint>{endpoint}</Endpoint>\
         <Status>{status}</Status>\
         <EndpointType>{etype}</EndpointType>\
         {ctype}\
         {static_members}\
         {excluded_members}\
         {arn}",
        id = optional(
            "DBClusterEndpointIdentifier",
            &e.db_cluster_endpoint_identifier
        ),
        cluster = xml_escape(&e.db_cluster_identifier),
        rid = optional(
            "DBClusterEndpointResourceIdentifier",
            &e.db_cluster_endpoint_resource_identifier
        ),
        endpoint = xml_escape(&e.endpoint),
        status = xml_escape(&e.status),
        etype = xml_escape(&e.endpoint_type),
        ctype = optional("CustomEndpointType", &e.custom_endpoint_type),
        arn = optional("DBClusterEndpointArn", &e.db_cluster_endpoint_arn),
    )
}

pub(crate) fn db_instance(i: &DbInstance) -> String {
    let logs = string_list(
        "EnabledCloudwatchLogsExports",
        "member",
        &i.enabled_cloudwatch_logs_exports,
    );
    let kms = i
        .kms_key_id
        .as_ref()
        .map(|k| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(k)))
        .unwrap_or_default();
    format!(
        "<DBInstanceIdentifier>{id}</DBInstanceIdentifier>\
         <DBInstanceArn>{arn}</DBInstanceArn>\
         <DBInstanceClass>{class}</DBInstanceClass>\
         <Engine>{engine}</Engine>\
         <EngineVersion>{ev}</EngineVersion>\
         <DBInstanceStatus>{status}</DBInstanceStatus>\
         <DBClusterIdentifier>{cluster}</DBClusterIdentifier>\
         <Endpoint><Address>{addr}</Address><Port>{port}</Port><HostedZoneId>Z1NEPTUNE0000</HostedZoneId></Endpoint>\
         <AvailabilityZone>{az}</AvailabilityZone>\
         <PubliclyAccessible>{pub_acc}</PubliclyAccessible>\
         <AutoMinorVersionUpgrade>{amvu}</AutoMinorVersionUpgrade>\
         <PromotionTier>{tier}</PromotionTier>\
         <DbiResourceId>{rid}</DbiResourceId>\
         <CACertificateIdentifier>{ca}</CACertificateIdentifier>\
         <PreferredBackupWindow>{pbw}</PreferredBackupWindow>\
         <PreferredMaintenanceWindow>{pmw}</PreferredMaintenanceWindow>\
         <BackupRetentionPeriod>{brp}</BackupRetentionPeriod>\
         <StorageEncrypted>{enc}</StorageEncrypted>\
         {kms}\
         <DBSubnetGroup><DBSubnetGroupName>{subnet}</DBSubnetGroupName><SubnetGroupStatus>Complete</SubnetGroupStatus><Subnets/></DBSubnetGroup>\
         <InstanceCreateTime>{ict}</InstanceCreateTime>\
         <VpcSecurityGroups/>\
         <StatusInfos/>\
         <PendingModifiedValues/>\
         {logs}",
        id = xml_escape(&i.db_instance_identifier),
        arn = xml_escape(&i.db_instance_arn),
        class = xml_escape(&i.db_instance_class),
        engine = xml_escape(&i.engine),
        ev = xml_escape(&i.engine_version),
        status = xml_escape(&i.status),
        cluster = xml_escape(&i.db_cluster_identifier),
        addr = xml_escape(&i.endpoint_address),
        port = i.port,
        az = xml_escape(&i.availability_zone),
        pub_acc = i.publicly_accessible,
        amvu = i.auto_minor_version_upgrade,
        tier = i.promotion_tier,
        rid = xml_escape(&i.dbi_resource_id),
        ca = xml_escape(&i.ca_certificate_identifier),
        pbw = xml_escape(&i.preferred_backup_window),
        pmw = xml_escape(&i.preferred_maintenance_window),
        brp = i.backup_retention_period,
        enc = i.storage_encrypted,
        subnet = xml_escape(&i.db_subnet_group),
        ict = ts(&i.instance_create_time),
    )
}

pub(crate) fn db_cluster_snapshot(s: &DbClusterSnapshot) -> String {
    let azs = string_list(
        "AvailabilityZones",
        "AvailabilityZone",
        &s.availability_zones,
    );
    let kms = s
        .kms_key_id
        .as_ref()
        .map(|k| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(k)))
        .unwrap_or_default();
    let source = s
        .source_db_cluster_snapshot_arn
        .as_ref()
        .map(|a| {
            format!(
                "<SourceDBClusterSnapshotArn>{}</SourceDBClusterSnapshotArn>",
                xml_escape(a)
            )
        })
        .unwrap_or_default();
    format!(
        "<DBClusterSnapshotIdentifier>{id}</DBClusterSnapshotIdentifier>\
         <DBClusterSnapshotArn>{arn}</DBClusterSnapshotArn>\
         <DBClusterIdentifier>{cluster}</DBClusterIdentifier>\
         <Status>{status}</Status>\
         <Engine>{engine}</Engine>\
         <EngineVersion>{ev}</EngineVersion>\
         <Port>{port}</Port>\
         <MasterUsername>{user}</MasterUsername>\
         <SnapshotType>{stype}</SnapshotType>\
         <StorageEncrypted>{enc}</StorageEncrypted>\
         {kms}\
         <PercentProgress>{pct}</PercentProgress>\
         <VpcId>{vpc}</VpcId>\
         <StorageType>{st}</StorageType>\
         <ClusterCreateTime>{cct}</ClusterCreateTime>\
         <SnapshotCreateTime>{sct}</SnapshotCreateTime>\
         {source}\
         {azs}",
        id = xml_escape(&s.db_cluster_snapshot_identifier),
        arn = xml_escape(&s.db_cluster_snapshot_arn),
        cluster = xml_escape(&s.db_cluster_identifier),
        status = xml_escape(&s.status),
        engine = xml_escape(&s.engine),
        ev = xml_escape(&s.engine_version),
        port = s.port,
        user = xml_escape(&s.master_username),
        stype = xml_escape(&s.snapshot_type),
        enc = s.storage_encrypted,
        pct = s.percent_progress,
        vpc = xml_escape(&s.vpc_id),
        st = xml_escape(&s.storage_type),
        cct = ts(&s.cluster_create_time),
        sct = ts(&s.snapshot_create_time),
    )
}

pub(crate) fn parameter(name: &str, p: &ParameterValue) -> String {
    format!(
        "<Parameter><ParameterName>{}</ParameterName><ParameterValue>{}</ParameterValue><Source>user</Source><ApplyType>dynamic</ApplyType><ApplyMethod>{}</ApplyMethod><DataType>string</DataType><IsModifiable>true</IsModifiable></Parameter>",
        xml_escape(name),
        xml_escape(&p.value),
        xml_escape(&p.apply_method),
    )
}

pub(crate) fn db_cluster_parameter_group(g: &DbClusterParameterGroup) -> String {
    format!(
        "<DBClusterParameterGroupName>{name}</DBClusterParameterGroupName>\
         <DBClusterParameterGroupArn>{arn}</DBClusterParameterGroupArn>\
         <DBParameterGroupFamily>{fam}</DBParameterGroupFamily>\
         <Description>{desc}</Description>",
        name = xml_escape(&g.db_cluster_parameter_group_name),
        arn = xml_escape(&g.db_cluster_parameter_group_arn),
        fam = xml_escape(&g.db_parameter_group_family),
        desc = xml_escape(&g.description),
    )
}

pub(crate) fn db_parameter_group(g: &DbParameterGroup) -> String {
    format!(
        "<DBParameterGroupName>{name}</DBParameterGroupName>\
         <DBParameterGroupArn>{arn}</DBParameterGroupArn>\
         <DBParameterGroupFamily>{fam}</DBParameterGroupFamily>\
         <Description>{desc}</Description>",
        name = xml_escape(&g.db_parameter_group_name),
        arn = xml_escape(&g.db_parameter_group_arn),
        fam = xml_escape(&g.db_parameter_group_family),
        desc = xml_escape(&g.description),
    )
}

pub(crate) fn db_subnet_group(g: &DbSubnetGroup) -> String {
    let subnets: String = g
        .subnets
        .iter()
        .map(|s| {
            format!(
                "<Subnet><SubnetIdentifier>{}</SubnetIdentifier><SubnetAvailabilityZone><Name>{}</Name></SubnetAvailabilityZone><SubnetStatus>{}</SubnetStatus></Subnet>",
                xml_escape(&s.subnet_identifier),
                xml_escape(&s.availability_zone),
                xml_escape(&s.status),
            )
        })
        .collect();
    format!(
        "<DBSubnetGroupName>{name}</DBSubnetGroupName>\
         <DBSubnetGroupArn>{arn}</DBSubnetGroupArn>\
         <DBSubnetGroupDescription>{desc}</DBSubnetGroupDescription>\
         <VpcId>{vpc}</VpcId>\
         <SubnetGroupStatus>{status}</SubnetGroupStatus>\
         <Subnets>{subnets}</Subnets>",
        name = xml_escape(&g.db_subnet_group_name),
        arn = xml_escape(&g.db_subnet_group_arn),
        desc = xml_escape(&g.db_subnet_group_description),
        vpc = xml_escape(&g.vpc_id),
        status = xml_escape(&g.subnet_group_status),
    )
}

pub(crate) fn global_cluster(g: &GlobalCluster) -> String {
    let members: String = g
        .members
        .iter()
        .map(|m| {
            format!(
                "<GlobalClusterMember><DBClusterArn>{}</DBClusterArn><IsWriter>{}</IsWriter><Readers/></GlobalClusterMember>",
                xml_escape(&m.db_cluster_arn),
                m.is_writer
            )
        })
        .collect();
    let dbname = g
        .database_name
        .as_ref()
        .map(|d| format!("<DatabaseName>{}</DatabaseName>", xml_escape(d)))
        .unwrap_or_default();
    format!(
        "<GlobalClusterIdentifier>{id}</GlobalClusterIdentifier>\
         <GlobalClusterArn>{arn}</GlobalClusterArn>\
         <GlobalClusterResourceId>{rid}</GlobalClusterResourceId>\
         <Status>{status}</Status>\
         <Engine>{engine}</Engine>\
         <EngineVersion>{ev}</EngineVersion>\
         {dbname}\
         <StorageEncrypted>{enc}</StorageEncrypted>\
         <DeletionProtection>{del}</DeletionProtection>\
         <GlobalClusterMembers>{members}</GlobalClusterMembers>",
        id = xml_escape(&g.global_cluster_identifier),
        arn = xml_escape(&g.global_cluster_arn),
        rid = xml_escape(&g.global_cluster_resource_id),
        status = xml_escape(&g.status),
        engine = xml_escape(&g.engine),
        ev = xml_escape(&g.engine_version),
        enc = g.storage_encrypted,
        del = g.deletion_protection,
    )
}

pub(crate) fn event_subscription(s: &EventSubscription) -> String {
    let source_ids = string_list("SourceIdsList", "SourceId", &s.source_ids);
    let categories = string_list("EventCategoriesList", "EventCategory", &s.event_categories);
    let source_type = s
        .source_type
        .as_ref()
        .map(|t| format!("<SourceType>{}</SourceType>", xml_escape(t)))
        .unwrap_or_default();
    format!(
        "<CustomerAwsId>{cust}</CustomerAwsId>\
         <CustSubscriptionId>{name}</CustSubscriptionId>\
         <EventSubscriptionArn>{arn}</EventSubscriptionArn>\
         <SnsTopicArn>{topic}</SnsTopicArn>\
         <Status>{status}</Status>\
         <SubscriptionCreationTime>{ct}</SubscriptionCreationTime>\
         <Enabled>{enabled}</Enabled>\
         {source_type}\
         {source_ids}\
         {categories}",
        cust = xml_escape(&s.customer_aws_id),
        name = xml_escape(&s.subscription_name),
        arn = xml_escape(&s.event_subscription_arn),
        topic = xml_escape(&s.sns_topic_arn),
        status = xml_escape(&s.status),
        ct = xml_escape(&s.subscription_creation_time),
        enabled = s.enabled,
    )
}
