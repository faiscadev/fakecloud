use crate::state::{AttributeValue, DynamoDbStreamRecord, DynamoTable, StreamRecord};
use chrono::Utc;
use std::collections::HashMap;
use uuid::Uuid;

/// Generate a stream record for a table mutation.
/// This should be called after the mutation is applied.
pub fn generate_stream_record(
    table: &DynamoTable,
    event_name: &str, // INSERT, MODIFY, REMOVE
    keys: HashMap<String, AttributeValue>,
    old_image: Option<HashMap<String, AttributeValue>>,
    new_image: Option<HashMap<String, AttributeValue>>,
) -> Option<StreamRecord> {
    if !table.stream_enabled {
        return None;
    }

    let stream_view_type = table.stream_view_type.as_ref()?;

    // Filter images based on stream view type
    let (filtered_old, filtered_new) = match stream_view_type.as_str() {
        "KEYS_ONLY" => (None, None),
        "NEW_IMAGE" => (None, new_image),
        "OLD_IMAGE" => (old_image, None),
        "NEW_AND_OLD_IMAGES" => (old_image, new_image),
        _ => (None, None),
    };

    // Calculate size
    let size_bytes = serde_json::to_vec(&keys).ok()?.len() as i64
        + filtered_old
            .as_ref()
            .and_then(|img| serde_json::to_vec(img).ok())
            .map(|v| v.len() as i64)
            .unwrap_or(0)
        + filtered_new
            .as_ref()
            .and_then(|img| serde_json::to_vec(img).ok())
            .map(|v| v.len() as i64)
            .unwrap_or(0);

    let event_id = Uuid::new_v4().to_string();
    let sequence_number = Utc::now().timestamp_nanos_opt()?.to_string();

    Some(StreamRecord {
        event_id,
        event_name: event_name.to_string(),
        event_version: "1.1".to_string(),
        event_source: "aws:dynamodb".to_string(),
        aws_region: "us-east-1".to_string(), // TODO: get from state
        dynamodb: DynamoDbStreamRecord {
            keys,
            new_image: filtered_new,
            old_image: filtered_old,
            sequence_number,
            size_bytes,
            stream_view_type: stream_view_type.clone(),
        },
        event_source_arn: table.stream_arn.clone().unwrap_or_default(),
        timestamp: Utc::now(),
    })
}

/// Add a stream record to the table's stream.
/// Records are retained for 24 hours.
pub fn add_stream_record(table: &mut DynamoTable, record: StreamRecord) {
    let mut records = table.stream_records.write();
    records.push(record);

    // Clean up records older than 24 hours
    let cutoff = Utc::now() - chrono::Duration::hours(24);
    records.retain(|r| r.timestamp > cutoff);
}
