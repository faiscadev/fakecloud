//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `dynamodb`.

use super::*;

fn cfn_bool(v: &serde_json::Value) -> Option<bool> {
    v.as_bool().or_else(|| match v.as_str() {
        Some("true") => Some(true),
        Some("false") => Some(false),
        _ => None,
    })
}

/// Mint a stream ARN for `table`, labelled with the current time as
/// DynamoDB labels a newly enabled stream.
fn new_stream_arn(table: &DynamoTable) -> String {
    format!(
        "{}/stream/{}",
        table.arn,
        Utc::now().format("%Y-%m-%dT%H:%M:%S.%3f")
    )
}

/// Apply the `AWS::DynamoDB::Table` properties that DynamoDB configures
/// through operations other than CreateTable -- `TimeToLiveSpecification`
/// (UpdateTimeToLive), `PointInTimeRecoverySpecification`
/// (UpdateContinuousBackups), `KinesisStreamSpecification`
/// (EnableKinesisStreamingDestination) and, on update, `StreamSpecification`
/// (UpdateTable) -- onto the same table fields those operations write, so
/// DescribeTimeToLive / DescribeContinuousBackups /
/// DescribeKinesisStreamingDestination / DescribeTable reflect the template.
///
/// On update a property the template no longer sets reverts to its default
/// (TTL, PITR, Kinesis streaming and the table stream are turned off), as a
/// CloudFormation update that drops a property does.
fn apply_cfn_table_settings(
    table: &mut DynamoTable,
    props: &serde_json::Value,
    is_update: bool,
) -> Result<(), String> {
    // --- TimeToLiveSpecification ---
    match props.get("TimeToLiveSpecification") {
        Some(spec) => {
            let enabled = spec
                .get("Enabled")
                .and_then(cfn_bool)
                .ok_or("TimeToLiveSpecification.Enabled is required")?;
            let attr = spec.get("AttributeName").and_then(|v| v.as_str());
            if enabled {
                let attr = attr.filter(|a| !a.is_empty()).ok_or(
                    "TimeToLiveSpecification.AttributeName is required when TTL is enabled",
                )?;
                table.ttl_attribute = Some(attr.to_string());
                table.ttl_enabled = true;
            } else {
                if let Some(attr) = attr.filter(|a| !a.is_empty()) {
                    table.ttl_attribute = Some(attr.to_string());
                }
                table.ttl_enabled = false;
            }
        }
        None if is_update => table.ttl_enabled = false,
        None => {}
    }

    // --- PointInTimeRecoverySpecification ---
    match props.get("PointInTimeRecoverySpecification") {
        Some(spec) => {
            table.set_pitr(
                spec.get("PointInTimeRecoveryEnabled")
                    .and_then(cfn_bool)
                    .unwrap_or(false),
            );
        }
        None if is_update => table.set_pitr(false),
        None => {}
    }

    // --- KinesisStreamSpecification ---
    let wanted = props.get("KinesisStreamSpecification");
    let wanted_arn = match wanted {
        Some(spec) => Some(
            spec.get("StreamArn")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .ok_or("KinesisStreamSpecification.StreamArn is required")?,
        ),
        None => None,
    };
    if wanted.is_some() || is_update {
        // Any other active destination is turned off, as disabling it with
        // DisableKinesisStreamingDestination would.
        for dest in table.kinesis_destinations.iter_mut() {
            if Some(dest.stream_arn.as_str()) != wanted_arn && dest.destination_status == "ACTIVE" {
                dest.destination_status = "DISABLED".to_string();
            }
        }
    }
    if let (Some(spec), Some(arn)) = (wanted, wanted_arn) {
        let precision = spec
            .get("ApproximateCreationDateTimePrecision")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        match table
            .kinesis_destinations
            .iter_mut()
            .find(|d| d.stream_arn == arn)
        {
            Some(dest) => {
                dest.destination_status = "ACTIVE".to_string();
                dest.approximate_creation_date_time_precision = precision;
            }
            None => table.kinesis_destinations.push(KinesisDestination {
                stream_arn: arn.to_string(),
                destination_status: "ACTIVE".to_string(),
                approximate_creation_date_time_precision: precision,
            }),
        }
    }

    // --- StreamSpecification (update; create sets it up front) ---
    if is_update {
        let (enabled, view_type) = match props.get("StreamSpecification") {
            Some(spec) => {
                let view_type = spec
                    .get("StreamViewType")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let enabled = spec
                    .get("StreamEnabled")
                    .and_then(cfn_bool)
                    .unwrap_or(view_type.is_some());
                (enabled, view_type)
            }
            None => (false, None),
        };
        if enabled {
            // Turning the stream on, or changing its view type (which
            // CloudFormation does by disabling and re-enabling it), starts a
            // new stream with a new ARN.
            let view_changed = view_type.is_some() && view_type != table.stream_view_type;
            if !table.stream_enabled || view_changed || table.stream_arn.is_none() {
                table.stream_arn = Some(new_stream_arn(table));
            }
            table.stream_enabled = true;
            if view_type.is_some() {
                table.stream_view_type = view_type;
            }
        } else {
            // Like UpdateTable, disabling keeps the last stream ARN visible as
            // LatestStreamArn.
            table.stream_enabled = false;
        }
    }
    Ok(())
}

impl ResourceProvisioner {
    pub(super) fn get_att_dynamodb_table(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.dynamodb_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let table = state.tables.get(physical_id)?;
        match attribute {
            "Arn" => Some(table.arn.clone()),
            "StreamArn" => table.stream_arn.clone(),
            _ => None,
        }
    }

    // --- DynamoDB ---

    pub(super) fn create_dynamodb_table(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let table_name = props
            .get("TableName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name);

        let mut key_schema = Vec::new();
        if let Some(ks) = props.get("KeySchema").and_then(|v| v.as_array()) {
            for item in ks {
                let attr_name = item
                    .get("AttributeName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let key_type = item
                    .get("KeyType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("HASH")
                    .to_string();
                key_schema.push(KeySchemaElement {
                    attribute_name: attr_name,
                    key_type,
                });
            }
        }

        let mut attribute_definitions = Vec::new();
        if let Some(defs) = props.get("AttributeDefinitions").and_then(|v| v.as_array()) {
            for item in defs {
                let attr_name = item
                    .get("AttributeName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let attr_type = item
                    .get("AttributeType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("S")
                    .to_string();
                attribute_definitions.push(AttributeDefinition {
                    attribute_name: attr_name,
                    attribute_type: attr_type,
                });
            }
        }

        // CloudFormation's BillingMode default is PROVISIONED (SAM's
        // SimpleTable sets PAY_PER_REQUEST itself during the transform), and
        // a provisioned table must state both capacity units, as CreateTable
        // requires.
        let billing_mode = props
            .get("BillingMode")
            .and_then(|v| v.as_str())
            .unwrap_or("PROVISIONED")
            .to_string();

        let provisioned_throughput = if billing_mode == "PROVISIONED" {
            let units = |key: &str| {
                props
                    .get("ProvisionedThroughput")
                    .and_then(|pt| pt.get(key))
                    .and_then(cfn_as_i64)
            };
            match (units("ReadCapacityUnits"), units("WriteCapacityUnits")) {
                (Some(read), Some(write)) => ProvisionedThroughput {
                    read_capacity_units: read,
                    write_capacity_units: write,
                },
                _ => {
                    return Err(
                        "One or more parameter values were invalid: ReadCapacityUnits and \
                         WriteCapacityUnits must both be specified when BillingMode is PROVISIONED"
                            .to_string(),
                    )
                }
            }
        } else {
            ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            }
        };

        // Parse StreamSpecification from CloudFormation properties
        let (stream_enabled, stream_view_type) =
            if let Some(stream_spec) = props.get("StreamSpecification") {
                let view_type = stream_spec
                    .get("StreamViewType")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let enabled = stream_spec
                    .get("StreamEnabled")
                    .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
                    // If StreamViewType is set, treat streams as enabled even if StreamEnabled is missing
                    .unwrap_or(view_type.is_some());
                (enabled, view_type)
            } else {
                (false, None)
            };

        let deletion_protection_enabled = props
            .get("DeletionProtectionEnabled")
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
            .unwrap_or(false);

        let on_demand_throughput = props
            .get("OnDemandThroughput")
            .map(|odt| OnDemandThroughput {
                max_read_request_units: odt
                    .get("MaxReadRequestUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1),
                max_write_request_units: odt
                    .get("MaxWriteRequestUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1),
            });

        let mut __ddb_mas = self.dynamodb_state.write();
        let state = __ddb_mas.get_or_create(&self.account_id);
        if state.tables.contains_key(table_name) {
            return Err(resource_already_exists("AWS::DynamoDB::Table", table_name));
        }
        let arn = fakecloud_dynamodb::table_arn(&self.region, &self.account_id, table_name);

        let stream_arn = if stream_enabled {
            Some(format!(
                "{}/stream/{}",
                arn,
                Utc::now().format("%Y-%m-%dT%H:%M:%S.%3f")
            ))
        } else {
            None
        };
        let stream_arn_attr = stream_arn.clone();

        // Secondary indexes, SSE and tags: parse via the same dynamodb helpers
        // the native CreateTable uses, so a CFN/SAM table carries its GSIs/LSIs
        // (otherwise a Query/Scan on the index name fails at runtime), its
        // encryption config and its tags — instead of provisioning them empty.
        let null = serde_json::Value::Null;
        let gsi = fakecloud_dynamodb::parse_gsi(
            props.get("GlobalSecondaryIndexes").unwrap_or(&null),
            &billing_mode,
        );
        let lsi =
            fakecloud_dynamodb::parse_lsi(props.get("LocalSecondaryIndexes").unwrap_or(&null));
        let tags = props
            .get("Tags")
            .map(fakecloud_dynamodb::parse_tags)
            .unwrap_or_default();
        let (sse_type, sse_kms_key_arn) = match props.get("SSESpecification") {
            Some(sse_spec)
                if sse_spec
                    .get("SSEEnabled")
                    .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
                    .unwrap_or(false) =>
            {
                let sse_type = sse_spec
                    .get("SSEType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("KMS")
                    .to_string();
                let kms_key = sse_spec
                    .get("KMSMasterKeyId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                (Some(sse_type), kms_key)
            }
            _ => (None, None),
        };

        // The rows and the key index that has to stay in step with them are
        // the DynamoDB crate's to manage, so a new table comes from its
        // constructor and the declared properties are set on top.
        let mut table = DynamoTable::new(
            table_name.to_string(),
            arn.clone(),
            Uuid::new_v4().to_string().replace('-', ""),
            key_schema,
            attribute_definitions,
            provisioned_throughput,
            billing_mode,
            Utc::now(),
        );
        table.gsi = gsi;
        table.lsi = lsi;
        table.tags = tags;
        table.stream_enabled = stream_enabled;
        table.stream_view_type = stream_view_type;
        table.stream_arn = stream_arn;
        table.sse_type = sse_type;
        table.sse_kms_key_arn = sse_kms_key_arn;
        table.deletion_protection_enabled = deletion_protection_enabled;
        table.on_demand_throughput = on_demand_throughput;
        table.table_class = props
            .get("TableClass")
            .and_then(|v| v.as_str())
            .unwrap_or("STANDARD")
            .to_string();
        apply_cfn_table_settings(&mut table, props, false)?;
        let table = table;

        state.tables.insert(table_name.to_string(), table);
        let mut result = ProvisionResult::new(arn.clone()).with("Arn", arn);
        if let Some(stream_arn_value) = stream_arn_attr {
            result = result.with("StreamArn", stream_arn_value);
        }
        Ok(result)
    }

    /// Apply a CFN property update to an existing DynamoDB table in place.
    /// `BillingMode`, `ProvisionedThroughput`, `GlobalSecondaryIndexes`,
    /// `OnDemandThroughput`, `DeletionProtectionEnabled`, `TableClass` and
    /// `SSESpecification` are all update-without-replacement in real
    /// CloudFormation, so a stack update must reach the table and be reflected
    /// by `DescribeTable` instead of being silently dropped. Key schema and
    /// attribute definitions are replacement-only and left untouched here.
    pub(super) fn update_dynamodb_table(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = &existing.physical_id;

        let mut __ddb_mas = self.dynamodb_state.write();
        let state = __ddb_mas.get_or_create(&self.account_id);
        let table = state
            .tables
            .values_mut()
            .find(|t| &t.arn == arn)
            .ok_or_else(|| format!("DynamoDB table {arn} not yet provisioned"))?;

        if let Some(billing_mode) = props.get("BillingMode").and_then(|v| v.as_str()) {
            table.billing_mode = billing_mode.to_string();
        }
        // Provisioned throughput only applies under PROVISIONED billing; when
        // switching to PAY_PER_REQUEST the units go to zero, matching Describe.
        if table.billing_mode == "PROVISIONED" {
            if let Some(pt) = props.get("ProvisionedThroughput") {
                table.provisioned_throughput = ProvisionedThroughput {
                    read_capacity_units: pt
                        .get("ReadCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(table.provisioned_throughput.read_capacity_units),
                    write_capacity_units: pt
                        .get("WriteCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(table.provisioned_throughput.write_capacity_units),
                };
            }
        } else {
            table.provisioned_throughput = ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            };
        }
        if let Some(gsi) = props.get("GlobalSecondaryIndexes") {
            table.gsi = fakecloud_dynamodb::parse_gsi(gsi, &table.billing_mode);
        }
        if let Some(odt) = props.get("OnDemandThroughput") {
            table.on_demand_throughput = Some(OnDemandThroughput {
                max_read_request_units: odt
                    .get("MaxReadRequestUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1),
                max_write_request_units: odt
                    .get("MaxWriteRequestUnits")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1),
            });
        }
        if let Some(dp) = props
            .get("DeletionProtectionEnabled")
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
        {
            table.deletion_protection_enabled = dp;
        }
        if let Some(class) = props.get("TableClass").and_then(|v| v.as_str()) {
            table.table_class = class.to_string();
        }
        if let Some(sse_spec) = props.get("SSESpecification") {
            let enabled = sse_spec
                .get("SSEEnabled")
                .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
                .unwrap_or(false);
            if enabled {
                table.sse_type = Some(
                    sse_spec
                        .get("SSEType")
                        .and_then(|v| v.as_str())
                        .unwrap_or("KMS")
                        .to_string(),
                );
                table.sse_kms_key_arn = sse_spec
                    .get("KMSMasterKeyId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            } else {
                table.sse_type = None;
                table.sse_kms_key_arn = None;
            }
        }

        apply_cfn_table_settings(table, props, true)?;

        let mut result = ProvisionResult::new(arn.clone()).with("Arn", arn.clone());
        if let Some(stream_arn) = table.stream_arn.clone().filter(|_| table.stream_enabled) {
            result = result.with("StreamArn", stream_arn);
        }
        Ok(result)
    }

    pub(super) fn delete_dynamodb_table(&self, physical_id: &str) -> Result<(), String> {
        let mut __ddb_mas = self.dynamodb_state.write();
        let state = __ddb_mas.get_or_create(&self.account_id);
        // physical_id is the ARN; find the table name
        let table_name = state
            .tables
            .iter()
            .find(|(_, t)| t.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = table_name {
            state.tables.remove(&name);
        }
        Ok(())
    }
}
