//! `xml_renderers` concerns from rds/extras.rs (audit-2026-05-19).

use super::*;

/// Render a JSON string array as an AWS query `<Wrapper><member>..</member></Wrapper>`
/// list, or a self-closing `<Wrapper/>` when empty/absent.
pub(super) fn json_str_array_xml(v: &Value, wrapper: &str) -> String {
    let members: Vec<&str> = v
        .as_array()
        .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
        .unwrap_or_default();
    if members.is_empty() {
        return format!("<{wrapper}/>");
    }
    let body: String = members
        .iter()
        .map(|m| format!("<member>{}</member>", xml_escape(m)))
        .collect();
    format!("<{wrapper}>{body}</{wrapper}>")
}

pub(super) fn db_cluster_member_xml(v: &Value) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "          <DBClusterIdentifier>{}</DBClusterIdentifier>\n",
        xml_escape(v["DBClusterIdentifier"].as_str().unwrap_or(""))
    ));
    out.push_str(&format!(
        "          <DBClusterArn>{}</DBClusterArn>\n",
        xml_escape(v["DBClusterArn"].as_str().unwrap_or(""))
    ));
    out.push_str(&format!(
        "          <Status>{}</Status>\n",
        xml_escape(v["Status"].as_str().unwrap_or("available"))
    ));
    if let Some(s) = v["Engine"].as_str() {
        out.push_str(&format!("          <Engine>{}</Engine>\n", xml_escape(s)));
    }
    if let Some(s) = v["EngineVersion"].as_str() {
        out.push_str(&format!(
            "          <EngineVersion>{}</EngineVersion>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["MasterUsername"].as_str() {
        out.push_str(&format!(
            "          <MasterUsername>{}</MasterUsername>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DatabaseName"].as_str() {
        out.push_str(&format!(
            "          <DatabaseName>{}</DatabaseName>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["Endpoint"].as_str() {
        out.push_str(&format!(
            "          <Endpoint>{}</Endpoint>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["ReaderEndpoint"].as_str() {
        out.push_str(&format!(
            "          <ReaderEndpoint>{}</ReaderEndpoint>\n",
            xml_escape(s)
        ));
    }
    if let Some(n) = v["Port"].as_i64() {
        out.push_str(&format!("          <Port>{}</Port>\n", n));
    }
    if let Some(n) = v["AllocatedStorage"].as_i64() {
        out.push_str(&format!(
            "          <AllocatedStorage>{}</AllocatedStorage>\n",
            n
        ));
    }
    if let Some(n) = v["BackupRetentionPeriod"].as_i64() {
        out.push_str(&format!(
            "          <BackupRetentionPeriod>{}</BackupRetentionPeriod>\n",
            n
        ));
    }
    if let Some(b) = v["StorageEncrypted"].as_bool() {
        out.push_str(&format!(
            "          <StorageEncrypted>{}</StorageEncrypted>\n",
            b
        ));
    }
    if let Some(s) = v["KmsKeyId"].as_str() {
        out.push_str(&format!(
            "          <KmsKeyId>{}</KmsKeyId>\n",
            xml_escape(s)
        ));
    }
    if let Some(b) = v["DeletionProtection"].as_bool() {
        out.push_str(&format!(
            "          <DeletionProtection>{}</DeletionProtection>\n",
            b
        ));
    }
    if let Some(s) = v["DBSubnetGroup"].as_str() {
        out.push_str(&format!(
            "          <DBSubnetGroup>{}</DBSubnetGroup>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DbClusterResourceId"].as_str() {
        out.push_str(&format!(
            "          <DbClusterResourceId>{}</DbClusterResourceId>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["CloneGroupId"].as_str() {
        out.push_str(&format!(
            "          <CloneGroupId>{}</CloneGroupId>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["ClusterCreateTime"].as_str() {
        out.push_str(&format!(
            "          <ClusterCreateTime>{}</ClusterCreateTime>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["WriterDBInstanceIdentifier"].as_str() {
        out.push_str(&format!(
            "          <WriterDBInstanceIdentifier>{}</WriterDBInstanceIdentifier>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["BacktrackTo"].as_str() {
        out.push_str(&format!(
            "          <BacktrackTo>{}</BacktrackTo>\n",
            xml_escape(s)
        ));
    }
    if let Some(n) = v["BacktrackConsumedChangeRecords"].as_i64() {
        out.push_str(&format!(
            "          <BacktrackConsumedChangeRecords>{}</BacktrackConsumedChangeRecords>\n",
            n
        ));
    }
    if let Some(s) = v["EarliestRestorableTime"].as_str() {
        out.push_str(&format!(
            "          <EarliestRestorableTime>{}</EarliestRestorableTime>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["LatestRestorableTime"].as_str() {
        out.push_str(&format!(
            "          <LatestRestorableTime>{}</LatestRestorableTime>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["PreferredBackupWindow"].as_str() {
        out.push_str(&format!(
            "          <PreferredBackupWindow>{}</PreferredBackupWindow>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["PreferredMaintenanceWindow"].as_str() {
        out.push_str(&format!(
            "          <PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DBClusterParameterGroup"].as_str() {
        out.push_str(&format!(
            "          <DBClusterParameterGroup>{}</DBClusterParameterGroup>\n",
            xml_escape(s)
        ));
    }
    if let Some(s) = v["DBClusterParameterGroupName"].as_str() {
        out.push_str(&format!(
            "          <DBClusterParameterGroup>{}</DBClusterParameterGroup>\n",
            xml_escape(s)
        ));
    }
    if let Some(arr) = v["VpcSecurityGroupIds"].as_array() {
        out.push_str("          <VpcSecurityGroups>\n");
        for sg in arr {
            if let Some(s) = sg.as_str() {
                out.push_str(&format!(
                    "            <VpcSecurityGroupMembership>\n              <VpcSecurityGroupId>{}</VpcSecurityGroupId>\n              <Status>active</Status>\n            </VpcSecurityGroupMembership>\n",
                    xml_escape(s)
                ));
            }
        }
        out.push_str("          </VpcSecurityGroups>\n");
    }
    if let Some(arr) = v["EnabledCloudwatchLogsExports"].as_array() {
        out.push_str("          <EnabledCloudwatchLogsExports>\n");
        for t in arr {
            if let Some(s) = t.as_str() {
                out.push_str(&format!("            <member>{}</member>\n", xml_escape(s)));
            }
        }
        out.push_str("          </EnabledCloudwatchLogsExports>\n");
    }
    if let Some(arr) = v["DBClusterMembers"].as_array() {
        out.push_str("          <DBClusterMembers>\n");
        for m in arr {
            let inst = m["DBInstanceIdentifier"].as_str().unwrap_or("");
            let writer = m["IsClusterWriter"].as_bool().unwrap_or(false);
            let pg = m["DBClusterParameterGroupStatus"]
                .as_str()
                .unwrap_or("in-sync");
            let promotion = m["PromotionTier"].as_i64().unwrap_or(1);
            out.push_str(&format!(
                "            <DBClusterMember>\n              <DBInstanceIdentifier>{}</DBInstanceIdentifier>\n              <IsClusterWriter>{}</IsClusterWriter>\n              <DBClusterParameterGroupStatus>{}</DBClusterParameterGroupStatus>\n              <PromotionTier>{}</PromotionTier>\n            </DBClusterMember>\n",
                xml_escape(inst),
                writer,
                xml_escape(pg),
                promotion
            ));
        }
        out.push_str("          </DBClusterMembers>\n");
    }

    // Scalar fields ModifyDBCluster (and CreateDBCluster) persist that the
    // hand-written renderer above omitted, so DescribeDBClusters reflects a
    // modify instead of always echoing the create-time defaults (bug-audit
    // 2026-06-20, 1.17). Keys already rendered above are deliberately excluded.
    for key in [
        "StorageType",
        "DBClusterInstanceClass",
        "EngineMode",
        "NetworkType",
        "MonitoringRoleArn",
        "PerformanceInsightsKMSKeyId",
        "Domain",
        "DomainIAMRoleName",
        "CACertificateIdentifier",
        "MasterUserSecretKmsKeyId",
        "AwsBackupRecoveryPointArn",
        "GlobalWriteForwardingStatus",
        "LocalWriteForwardingStatus",
        "ActivityStreamStatus",
        "ActivityStreamKmsKeyId",
        "ActivityStreamKinesisStreamName",
        "ActivityStreamMode",
    ] {
        if let Some(s) = v[key].as_str() {
            out.push_str(&format!("          <{key}>{}</{key}>\n", xml_escape(s)));
        }
    }
    for key in [
        "Iops",
        "BacktrackWindow",
        "MonitoringInterval",
        "PerformanceInsightsRetentionPeriod",
        // Aurora Serverless v1 current capacity, set by
        // ModifyCurrentDBClusterCapacity.
        "Capacity",
    ] {
        if let Some(n) = v[key].as_i64() {
            out.push_str(&format!("          <{key}>{n}</{key}>\n"));
        }
    }
    for key in [
        "IAMDatabaseAuthenticationEnabled",
        "CopyTagsToSnapshot",
        "AutoMinorVersionUpgrade",
        "HttpEndpointEnabled",
        "PerformanceInsightsEnabled",
        "ManageMasterUserPassword",
    ] {
        if let Some(b) = v[key].as_bool() {
            out.push_str(&format!("          <{key}>{b}</{key}>\n"));
        }
    }
    // ServerlessV2ScalingConfiguration is stored as flattened
    // `ServerlessV2ScalingConfiguration.{Min,Max}Capacity` keys; render it back
    // as the nested element AWS returns.
    let v2_min = v["ServerlessV2ScalingConfiguration.MinCapacity"].as_str();
    let v2_max = v["ServerlessV2ScalingConfiguration.MaxCapacity"].as_str();
    if v2_min.is_some() || v2_max.is_some() {
        out.push_str("          <ServerlessV2ScalingConfiguration>\n");
        if let Some(m) = v2_min {
            out.push_str(&format!(
                "            <MinCapacity>{}</MinCapacity>\n",
                xml_escape(m)
            ));
        }
        if let Some(m) = v2_max {
            out.push_str(&format!(
                "            <MaxCapacity>{}</MaxCapacity>\n",
                xml_escape(m)
            ));
        }
        out.push_str("          </ServerlessV2ScalingConfiguration>\n");
    }
    out
}

pub(super) fn cluster_snapshot_member_xml(v: &Value) -> String {
    let mut out = format!(
        "          <DBClusterSnapshotIdentifier>{}</DBClusterSnapshotIdentifier>\n          <DBClusterSnapshotArn>{}</DBClusterSnapshotArn>\n          <DBClusterIdentifier>{}</DBClusterIdentifier>\n          <Status>{}</Status>",
        xml_escape(v["DBClusterSnapshotIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterSnapshotArn"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    );
    // Echo the fields DescribeDBClusterSnapshots filters on, so a client
    // that narrowed by snapshot-type / engine can read those values back
    // off the result instead of getting blanks.
    out.push_str(&format!(
        "\n          <SnapshotType>{}</SnapshotType>",
        xml_escape(v["SnapshotType"].as_str().unwrap_or("manual"))
    ));
    if let Some(s) = v["Engine"].as_str() {
        out.push_str(&format!("\n          <Engine>{}</Engine>", xml_escape(s)));
    }
    if let Some(s) = v["EngineVersion"].as_str() {
        out.push_str(&format!(
            "\n          <EngineVersion>{}</EngineVersion>",
            xml_escape(s)
        ));
    }
    out
}

pub(super) fn cluster_pg_xml(name: &str, arn: &str, family: &str, description: &str) -> String {
    format!(
        "    <DBClusterParameterGroup>\n      <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>\n      <DBClusterParameterGroupArn>{}</DBClusterParameterGroupArn>\n      <DBParameterGroupFamily>{}</DBParameterGroupFamily>\n      <Description>{}</Description>\n    </DBClusterParameterGroup>",
        xml_escape(name), xml_escape(arn), xml_escape(family), xml_escape(description),
    )
}

pub(super) fn cluster_pg_member_xml(v: &Value) -> String {
    format!(
        "          <DBClusterParameterGroupName>{}</DBClusterParameterGroupName>\n          <DBClusterParameterGroupArn>{}</DBClusterParameterGroupArn>\n          <DBParameterGroupFamily>{}</DBParameterGroupFamily>\n          <Description>{}</Description>",
        xml_escape(v["DBClusterParameterGroupName"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterParameterGroupArn"].as_str().unwrap_or("")),
        xml_escape(v["DBParameterGroupFamily"].as_str().unwrap_or("")),
        xml_escape(v["Description"].as_str().unwrap_or("")),
    )
}

pub(super) fn cluster_endpoint_xml(v: &Value) -> String {
    format!(
        "          <DBClusterEndpointIdentifier>{}</DBClusterEndpointIdentifier>\n          <DBClusterIdentifier>{}</DBClusterIdentifier>\n          <Endpoint>{}</Endpoint>\n          <EndpointType>{}</EndpointType>\n          <Status>{}</Status>",
        xml_escape(v["DBClusterEndpointIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["DBClusterIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Endpoint"].as_str().unwrap_or("")),
        xml_escape(v["EndpointType"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

pub(super) fn proxy_xml(v: &Value) -> String {
    format!(
        "          <DBProxyName>{}</DBProxyName>\n          <DBProxyArn>{}</DBProxyArn>\n          <Status>{}</Status>\n          <EngineFamily>{}</EngineFamily>",
        xml_escape(v["DBProxyName"].as_str().unwrap_or("")),
        xml_escape(v["DBProxyArn"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
        xml_escape(v["EngineFamily"].as_str().unwrap_or("POSTGRESQL")),
    )
}

pub(super) fn security_group_xml(v: &Value) -> String {
    format!(
        "          <DBSecurityGroupName>{}</DBSecurityGroupName>\n          <DBSecurityGroupDescription>{}</DBSecurityGroupDescription>\n          <OwnerId>{}</OwnerId>",
        xml_escape(v["DBSecurityGroupName"].as_str().unwrap_or("")),
        xml_escape(v["DBSecurityGroupDescription"].as_str().unwrap_or("")),
        xml_escape(v["OwnerId"].as_str().unwrap_or("000000000000")),
    )
}

pub(super) fn option_group_xml(v: &Value) -> String {
    let options = v["Options"]
        .as_array()
        .map(|opts| {
            opts.iter()
                .map(|o| {
                    let mut inner = format!(
                        "<OptionName>{}</OptionName>",
                        xml_escape(o["OptionName"].as_str().unwrap_or(""))
                    );
                    let port = o.get("Port").and_then(|p| {
                        p.as_i64()
                            .or_else(|| p.as_str().and_then(|s| s.parse().ok()))
                    });
                    if let Some(p) = port {
                        inner.push_str(&format!("<Port>{p}</Port>"));
                    }
                    if let Some(ver) = o.get("OptionVersion").and_then(|x| x.as_str()) {
                        inner.push_str(&format!(
                            "<OptionVersion>{}</OptionVersion>",
                            xml_escape(ver)
                        ));
                    }
                    format!("<Option>{inner}</Option>")
                })
                .collect::<String>()
        })
        .unwrap_or_default();
    let options_xml = if options.is_empty() {
        "<Options/>".to_string()
    } else {
        format!("<Options>{options}</Options>")
    };
    format!(
        "          <OptionGroupName>{}</OptionGroupName>\n          <OptionGroupArn>{}</OptionGroupArn>\n          <EngineName>{}</EngineName>\n          <MajorEngineVersion>{}</MajorEngineVersion>\n          <OptionGroupDescription>{}</OptionGroupDescription>\n          <AllowsVpcAndNonVpcInstanceMemberships>true</AllowsVpcAndNonVpcInstanceMemberships>\n          {}",
        xml_escape(v["OptionGroupName"].as_str().unwrap_or("")),
        xml_escape(v["OptionGroupArn"].as_str().unwrap_or("")),
        xml_escape(v["EngineName"].as_str().unwrap_or("")),
        xml_escape(v["MajorEngineVersion"].as_str().unwrap_or("")),
        xml_escape(v["OptionGroupDescription"].as_str().unwrap_or("")),
        options_xml,
    )
}

pub(super) fn event_sub_xml(v: &Value) -> String {
    format!(
        "          <CustSubscriptionId>{}</CustSubscriptionId>\n          <CustomerAwsId>{}</CustomerAwsId>\n          <EventSubscriptionArn>{}</EventSubscriptionArn>\n          <SnsTopicArn>{}</SnsTopicArn>\n          <SourceType>{}</SourceType>\n          <Status>{}</Status>\n          <Enabled>{}</Enabled>\n          {}\n          {}",
        xml_escape(v["CustSubscriptionId"].as_str().unwrap_or("")),
        xml_escape(v["CustomerAwsId"].as_str().unwrap_or("")),
        xml_escape(v["EventSubscriptionArn"].as_str().unwrap_or("")),
        xml_escape(v["SnsTopicArn"].as_str().unwrap_or("")),
        xml_escape(v["SourceType"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("active")),
        v["Enabled"].as_bool().unwrap_or(true),
        json_str_array_xml(&v["EventCategoriesList"], "EventCategoriesList"),
        json_str_array_xml(&v["SourceIdsList"], "SourceIdsList"),
    )
}

pub(super) fn global_cluster_xml(v: &Value) -> String {
    format!(
        "          <GlobalClusterIdentifier>{}</GlobalClusterIdentifier>\n          <GlobalClusterArn>{}</GlobalClusterArn>\n          <GlobalClusterResourceId>{}</GlobalClusterResourceId>\n          <Endpoint>{}</Endpoint>\n          <Status>{}</Status>\n          <Engine>{}</Engine>\n          <EngineVersion>{}</EngineVersion>\n          <EngineLifecycleSupport>{}</EngineLifecycleSupport>\n          <DatabaseName>{}</DatabaseName>\n          <DeletionProtection>{}</DeletionProtection>\n          <StorageEncrypted>{}</StorageEncrypted>",
        xml_escape(v["GlobalClusterIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["GlobalClusterArn"].as_str().unwrap_or("")),
        xml_escape(v["GlobalClusterResourceId"].as_str().unwrap_or("")),
        xml_escape(v["Endpoint"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
        xml_escape(v["Engine"].as_str().unwrap_or("aurora-postgresql")),
        xml_escape(v["EngineVersion"].as_str().unwrap_or("")),
        xml_escape(v["EngineLifecycleSupport"].as_str().unwrap_or("open-source-rds-extended-support")),
        xml_escape(v["DatabaseName"].as_str().unwrap_or("")),
        v["DeletionProtection"].as_bool().unwrap_or(false),
        v["StorageEncrypted"].as_bool().unwrap_or(false),
    )
}

pub(super) fn db_proxy_target_xml(v: &Value) -> String {
    format!(
        "<member><RdsResourceId>{}</RdsResourceId><Type>{}</Type><Port>{}</Port><Endpoint>{}</Endpoint><TargetHealth><State>AVAILABLE</State></TargetHealth></member>",
        xml_escape(v["RdsResourceId"].as_str().unwrap_or("")),
        xml_escape(v["Type"].as_str().unwrap_or("RDS_INSTANCE")),
        v["Port"].as_i64().unwrap_or(3306),
        xml_escape(v["Endpoint"].as_str().unwrap_or("")),
    )
}

pub(super) fn integration_xml(v: &Value) -> String {
    format!(
        "          <IntegrationName>{}</IntegrationName>\n          <IntegrationArn>{}</IntegrationArn>\n          <Status>{}</Status>",
        xml_escape(v["IntegrationName"].as_str().unwrap_or("")),
        xml_escape(v["IntegrationArn"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("active")),
    )
}

pub(super) fn blue_green_xml(v: &Value) -> String {
    format!(
        "          <BlueGreenDeploymentIdentifier>{}</BlueGreenDeploymentIdentifier>\n          <BlueGreenDeploymentName>{}</BlueGreenDeploymentName>\n          <Status>{}</Status>",
        xml_escape(v["BlueGreenDeploymentIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["BlueGreenDeploymentName"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("AVAILABLE")),
    )
}

pub(super) fn shard_group_xml(v: &Value) -> String {
    let mut out = format!(
        "          <DBShardGroupIdentifier>{}</DBShardGroupIdentifier>\n          <Status>{}</Status>",
        xml_escape(v["DBShardGroupIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    );
    if let Some(s) = v["DBClusterIdentifier"].as_str() {
        out.push_str(&format!(
            "\n          <DBClusterIdentifier>{}</DBClusterIdentifier>",
            xml_escape(s)
        ));
    }
    // MaxACU / MinACU are serverless capacity units (doubles); ComputeRedundancy
    // is an integer. Render whichever the caller set on Create/Modify.
    for key in ["MaxACU", "MinACU"] {
        if let Some(n) = v[key].as_f64() {
            out.push_str(&format!("\n          <{key}>{n}</{key}>"));
        }
    }
    if let Some(n) = v["ComputeRedundancy"].as_i64() {
        out.push_str(&format!(
            "\n          <ComputeRedundancy>{n}</ComputeRedundancy>"
        ));
    }
    out
}

pub(super) fn engine_version_xml(v: &Value) -> String {
    format!(
        "    <DBEngineVersion>\n      <Engine>{}</Engine>\n      <EngineVersion>{}</EngineVersion>\n      <Status>{}</Status>\n    </DBEngineVersion>",
        xml_escape(v["Engine"].as_str().unwrap_or("")),
        xml_escape(v["EngineVersion"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

pub(super) fn tenant_db_xml(v: &Value) -> String {
    format!(
        "          <TenantDBName>{}</TenantDBName>\n          <Status>{}</Status>",
        xml_escape(v["TenantDBName"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("available")),
    )
}

pub(super) fn export_task_xml(v: &Value) -> String {
    format!(
        "          <ExportTaskIdentifier>{}</ExportTaskIdentifier>\n          <Status>{}</Status>",
        xml_escape(v["ExportTaskIdentifier"].as_str().unwrap_or("")),
        xml_escape(v["Status"].as_str().unwrap_or("STARTING")),
    )
}
