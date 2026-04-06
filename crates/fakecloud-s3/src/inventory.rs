use bytes::Bytes;
use chrono::Utc;
use md5::{Digest, Md5};

use crate::state::{S3Object, SharedS3State};

/// Parsed inventory destination from the inventory configuration XML.
struct InventoryDestination {
    bucket_arn: String,
    prefix: Option<String>,
}

/// Parse the destination from an `<InventoryConfiguration>` XML body.
fn parse_inventory_destination(xml: &str) -> Option<InventoryDestination> {
    let dest_start = xml.find("<Destination>")?;
    let dest_end = xml.find("</Destination>")?;
    let dest_body = &xml[dest_start + 13..dest_end];

    // Look for <S3BucketDestination>
    let s3_start = dest_body.find("<S3BucketDestination>")?;
    let s3_end = dest_body.find("</S3BucketDestination>")?;
    let s3_body = &dest_body[s3_start + 21..s3_end];

    let bucket_arn = extract_tag(s3_body, "Bucket")?;
    let prefix = extract_tag(s3_body, "Prefix");

    Some(InventoryDestination { bucket_arn, prefix })
}

/// Extract the bucket name from an ARN like `arn:aws:s3:::my-bucket`.
fn bucket_name_from_arn(arn: &str) -> Option<&str> {
    arn.strip_prefix("arn:aws:s3:::")
}

/// Generate an inventory report for a bucket and store it in the destination.
///
/// The report is a CSV with columns: Bucket, Key, Size, LastModifiedDate, ETag, StorageClass.
pub fn generate_inventory_report(state: &SharedS3State, source_bucket: &str, config_id: &str) {
    // Read the inventory config
    let config_xml = {
        let st = state.read();
        st.buckets
            .get(source_bucket)
            .and_then(|b| b.inventory_configs.get(config_id).cloned())
    };

    let config_xml = match config_xml {
        Some(c) => c,
        None => return,
    };

    let destination = match parse_inventory_destination(&config_xml) {
        Some(d) => d,
        None => return,
    };

    let dest_bucket_name = match bucket_name_from_arn(&destination.bucket_arn) {
        Some(name) => name.to_string(),
        None => return,
    };

    // Collect object data from source bucket
    let rows: Vec<String> = {
        let st = state.read();
        let bucket = match st.buckets.get(source_bucket) {
            Some(b) => b,
            None => return,
        };

        let mut csv_rows = vec![
            "\"Bucket\",\"Key\",\"Size\",\"LastModifiedDate\",\"ETag\",\"StorageClass\""
                .to_string(),
        ];

        for (key, obj) in &bucket.objects {
            if obj.is_delete_marker {
                continue;
            }
            csv_rows.push(format!(
                "{},{},{},{},{},{}",
                csv_escape(source_bucket),
                csv_escape(key),
                obj.size,
                csv_escape(
                    &obj.last_modified
                        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        .to_string()
                ),
                csv_escape(&obj.etag),
                csv_escape(&obj.storage_class),
            ));
        }

        csv_rows
    };

    let csv_content = rows.join("\n") + "\n";
    let data = Bytes::from(csv_content);
    let size = data.len() as u64;
    let etag = format!("{:x}", Md5::digest(&data));
    let now = Utc::now();

    let report_key = format!(
        "{}{}/{}/data/{}.csv",
        destination.prefix.as_deref().unwrap_or(""),
        source_bucket,
        config_id,
        now.format("%Y-%m-%dT%H-%M-%SZ"),
    );

    let report_object = S3Object {
        key: report_key.clone(),
        data,
        content_type: "text/csv".to_string(),
        etag,
        size,
        last_modified: now,
        metadata: Default::default(),
        storage_class: "STANDARD".to_string(),
        tags: Default::default(),
        acl_grants: vec![],
        acl_owner_id: None,
        parts_count: None,
        part_sizes: None,
        sse_algorithm: None,
        sse_kms_key_id: None,
        bucket_key_enabled: None,
        version_id: None,
        is_delete_marker: false,
        content_encoding: None,
        website_redirect_location: None,
        restore_ongoing: None,
        restore_expiry: None,
        checksum_algorithm: None,
        checksum_value: None,
        lock_mode: None,
        lock_retain_until: None,
        lock_legal_hold: None,
    };

    let mut st = state.write();
    if let Some(target) = st.buckets.get_mut(&dest_bucket_name) {
        target.objects.insert(report_key, report_object);
    }
}

/// Escape a value for inclusion in a CSV field.  If the value contains a
/// comma, double-quote, or newline it is wrapped in double quotes and any
/// embedded double quotes are doubled.
fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        format!("\"{value}\"")
    }
}

fn extract_tag(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)?;
    let content_start = start + open.len();
    let end = body[content_start..].find(&close)?;
    Some(body[content_start..content_start + end].trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_destination_from_inventory_config() {
        let xml = r#"<InventoryConfiguration>
            <Id>my-inv</Id>
            <Destination>
                <S3BucketDestination>
                    <Bucket>arn:aws:s3:::dest-bucket</Bucket>
                    <Format>CSV</Format>
                    <Prefix>inventory/</Prefix>
                </S3BucketDestination>
            </Destination>
            <IsEnabled>true</IsEnabled>
            <Schedule><Frequency>Daily</Frequency></Schedule>
            <IncludedObjectVersions>Current</IncludedObjectVersions>
        </InventoryConfiguration>"#;

        let dest = parse_inventory_destination(xml).unwrap();
        assert_eq!(dest.bucket_arn, "arn:aws:s3:::dest-bucket");
        assert_eq!(dest.prefix.as_deref(), Some("inventory/"));
    }

    #[test]
    fn bucket_name_from_arn_works() {
        assert_eq!(
            bucket_name_from_arn("arn:aws:s3:::my-bucket"),
            Some("my-bucket")
        );
        assert_eq!(bucket_name_from_arn("not-an-arn"), None);
    }

    #[test]
    fn csv_escape_plain_value() {
        assert_eq!(csv_escape("hello"), "\"hello\"");
    }

    #[test]
    fn csv_escape_value_with_comma() {
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
    }

    #[test]
    fn csv_escape_value_with_quotes() {
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn csv_escape_value_with_comma_and_quotes() {
        assert_eq!(csv_escape("a,\"b\""), "\"a,\"\"b\"\"\"");
    }
}
