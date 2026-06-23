//! dynamodb helpers `table_descriptions` concerns (audit-2026-05-19).

use super::*;

pub(crate) fn build_table_description_json(input: &TableDescriptionInput<'_>) -> Value {
    let TableDescriptionInput {
        arn,
        table_id,
        key_schema,
        attribute_definitions,
        provisioned_throughput,
        gsi,
        lsi,
        billing_mode,
        created_at,
        item_count,
        size_bytes,
        status,
        deletion_protection_enabled,
        on_demand_throughput,
    } = *input;
    let table_name = arn.rsplit('/').next().unwrap_or("");
    let creation_timestamp =
        created_at.timestamp() as f64 + created_at.timestamp_subsec_millis() as f64 / 1000.0;

    let ks: Vec<Value> = key_schema
        .iter()
        .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
        .collect();

    let ad: Vec<Value> = attribute_definitions
        .iter()
        .map(|a| json!({"AttributeName": a.attribute_name, "AttributeType": a.attribute_type}))
        .collect();

    let mut desc = json!({
        "TableName": table_name,
        "TableArn": arn,
        "TableId": table_id,
        "TableStatus": status,
        "KeySchema": ks,
        "AttributeDefinitions": ad,
        "CreationDateTime": creation_timestamp,
        "ItemCount": item_count,
        "TableSizeBytes": size_bytes,
        "BillingModeSummary": { "BillingMode": billing_mode },
        "DeletionProtectionEnabled": deletion_protection_enabled,
    });

    if billing_mode != "PAY_PER_REQUEST" {
        desc["ProvisionedThroughput"] = json!({
            "ReadCapacityUnits": provisioned_throughput.read_capacity_units,
            "WriteCapacityUnits": provisioned_throughput.write_capacity_units,
            "NumberOfDecreasesToday": 0,
        });
    } else {
        desc["ProvisionedThroughput"] = json!({
            "ReadCapacityUnits": 0,
            "WriteCapacityUnits": 0,
            "NumberOfDecreasesToday": 0,
        });
    }

    if let Some(odt) = on_demand_throughput {
        desc["OnDemandThroughput"] = json!({
            "MaxReadRequestUnits": odt.max_read_request_units,
            "MaxWriteRequestUnits": odt.max_write_request_units,
        });
    }

    // Terraform's AWS provider now waits on WarmThroughput after CreateTable.
    // Real AWS returns an ACTIVE warm throughput object for active tables,
    // including PAY_PER_REQUEST tables. Returning null keeps the provider in a
    // perpetual "still creating" loop.
    if status == "ACTIVE" {
        desc["WarmThroughput"] = json!({
            "ReadUnitsPerSecond": 0,
            "WriteUnitsPerSecond": 0,
            "Status": "ACTIVE",
        });
    }

    if !gsi.is_empty() {
        let gsi_json: Vec<Value> = gsi
            .iter()
            .map(|g| {
                let gks: Vec<Value> = g
                    .key_schema
                    .iter()
                    .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
                    .collect();
                let mut idx = json!({
                    "IndexName": g.index_name,
                    "KeySchema": gks,
                    "Projection": { "ProjectionType": g.projection.projection_type },
                    "IndexStatus": "ACTIVE",
                    "IndexArn": format!("{arn}/index/{}", g.index_name),
                    "ItemCount": 0,
                    "IndexSizeBytes": 0,
                });
                if !g.projection.non_key_attributes.is_empty() {
                    idx["Projection"]["NonKeyAttributes"] = json!(g.projection.non_key_attributes);
                }
                if let Some(ref pt) = g.provisioned_throughput {
                    idx["ProvisionedThroughput"] = json!({
                        "ReadCapacityUnits": pt.read_capacity_units,
                        "WriteCapacityUnits": pt.write_capacity_units,
                        "NumberOfDecreasesToday": 0,
                    });
                }
                if let Some(ref odt) = g.on_demand_throughput {
                    idx["OnDemandThroughput"] = json!({
                        "MaxReadRequestUnits": odt.max_read_request_units,
                        "MaxWriteRequestUnits": odt.max_write_request_units,
                    });
                }
                idx
            })
            .collect();
        desc["GlobalSecondaryIndexes"] = json!(gsi_json);
    }

    if !lsi.is_empty() {
        let lsi_json: Vec<Value> = lsi
            .iter()
            .map(|l| {
                let lks: Vec<Value> = l
                    .key_schema
                    .iter()
                    .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
                    .collect();
                let mut idx = json!({
                    "IndexName": l.index_name,
                    "KeySchema": lks,
                    "Projection": { "ProjectionType": l.projection.projection_type },
                    "IndexArn": format!("{arn}/index/{}", l.index_name),
                    "ItemCount": 0,
                    "IndexSizeBytes": 0,
                });
                if !l.projection.non_key_attributes.is_empty() {
                    idx["Projection"]["NonKeyAttributes"] = json!(l.projection.non_key_attributes);
                }
                idx
            })
            .collect();
        desc["LocalSecondaryIndexes"] = json!(lsi_json);
    }

    desc
}

pub(crate) fn build_table_description(table: &DynamoTable) -> Value {
    let mut desc = build_table_description_json(&TableDescriptionInput {
        arn: &table.arn,
        table_id: &table.table_id,
        key_schema: &table.key_schema,
        attribute_definitions: &table.attribute_definitions,
        provisioned_throughput: &table.provisioned_throughput,
        gsi: &table.gsi,
        lsi: &table.lsi,
        billing_mode: &table.billing_mode,
        created_at: table.created_at,
        item_count: table.item_count,
        size_bytes: table.size_bytes,
        status: &table.status,
        deletion_protection_enabled: table.deletion_protection_enabled,
        on_demand_throughput: table.on_demand_throughput.as_ref(),
    });

    // `LatestStreamArn` / `LatestStreamLabel` persist after a stream has
    // been created, even if streams are currently disabled — real AWS
    // keeps them for ~24h post-disable so DescribeTable callers can still
    // observe the last active stream. fakecloud keeps them for the
    // table's lifetime, which is sufficient for any single test run.
    if let Some(ref stream_arn) = table.stream_arn {
        desc["LatestStreamArn"] = json!(stream_arn);
        desc["LatestStreamLabel"] = json!(stream_arn.rsplit('/').next().unwrap_or(""));
    }
    // The `StreamSpecification` block is only present while streams are
    // actively enabled. When absent, the Terraform provider Read falls
    // through to the prior `stream_view_type` from its own state rather
    // than clearing it, which matches the diff behaviour the upstream
    // acceptance tests assert on.
    if table.stream_enabled {
        if let Some(ref view_type) = table.stream_view_type {
            desc["StreamSpecification"] = json!({
                "StreamEnabled": true,
                "StreamViewType": view_type,
            });
        }
    }

    // SSEDescription is only returned when the customer explicitly enabled
    // a KMS-backed SSE. Real AWS tables using the default AWS-owned key omit
    // this field entirely, and the Terraform provider's Read asserts
    // `server_side_encryption.#` == 0 in that case.
    if let Some(ref sse_type) = table.sse_type {
        let mut sse_desc = json!({
            "Status": "ENABLED",
            "SSEType": sse_type,
        });
        if let Some(ref key_arn) = table.sse_kms_key_arn {
            sse_desc["KMSMasterKeyArn"] = json!(key_arn);
        }
        desc["SSEDescription"] = sse_desc;
    }

    // TableClassSummary echoes the storage class. Real AWS returns it on every
    // DescribeTable; the Terraform provider reads `table_class` from here and
    // falls back to STANDARD when absent.
    desc["TableClassSummary"] = json!({ "TableClass": table.table_class });

    desc
}
