//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `dynamodb`.

use super::*;

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
        let table_name = props
            .get("TableName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

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

        let billing_mode = props
            .get("BillingMode")
            .and_then(|v| v.as_str())
            .unwrap_or("PAY_PER_REQUEST")
            .to_string();

        let provisioned_throughput = if billing_mode == "PROVISIONED" {
            if let Some(pt) = props.get("ProvisionedThroughput") {
                ProvisionedThroughput {
                    read_capacity_units: pt
                        .get("ReadCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(5),
                    write_capacity_units: pt
                        .get("WriteCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(5),
                }
            } else {
                ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
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
        let arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}",
            state.region, state.account_id, table_name
        );

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

        let table = DynamoTable {
            name: table_name.to_string(),
            arn: arn.clone(),
            table_id: Uuid::new_v4().to_string().replace('-', ""),
            key_schema,
            attribute_definitions,
            provisioned_throughput,
            items: Vec::new(),
            gsi,
            lsi,
            tags,
            created_at: Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode,
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled,
            stream_view_type,
            stream_arn,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type,
            sse_kms_key_arn,
            deletion_protection_enabled,
            on_demand_throughput,
            table_class: props
                .get("TableClass")
                .and_then(|v| v.as_str())
                .unwrap_or("STANDARD")
                .to_string(),
        };

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

        let mut result = ProvisionResult::new(arn.clone()).with("Arn", arn.clone());
        if let Some(stream_arn) = table.stream_arn.clone() {
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
