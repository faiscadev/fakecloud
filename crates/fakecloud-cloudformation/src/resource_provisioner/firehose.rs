//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `firehose`.

use super::*;

impl ResourceProvisioner {
    // --- Firehose ---

    pub(super) fn create_firehose_delivery_stream(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let arn = format!(
            "arn:aws:firehose:{}:{}:deliverystream/{}",
            self.region, self.account_id, name
        );
        let stream_type = props
            .get("DeliveryStreamType")
            .and_then(|v| v.as_str())
            .unwrap_or("DirectPut")
            .to_string();

        let has_s3 = props.get("S3DestinationConfiguration").is_some();
        let has_extended_s3 = props.get("ExtendedS3DestinationConfiguration").is_some();
        if has_s3 && has_extended_s3 {
            return Err("Only one of S3DestinationConfiguration or ExtendedS3DestinationConfiguration may be set".to_string());
        }
        let destination = Some(if let Some(s3) = props.get("S3DestinationConfiguration") {
            parse_firehose_s3_destination(s3)?
        } else if let Some(s3) = props.get("ExtendedS3DestinationConfiguration") {
            parse_firehose_s3_destination(s3)?
        } else {
            return Err("Delivery stream requires a destination configuration".to_string());
        });

        let mut tags = BTreeMap::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for tag in arr {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|v| v.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        let stream = DeliveryStream {
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type: stream_type.clone(),
            created_at: Utc::now(),
            last_update: Utc::now(),
            version_id: "1".to_string(),
            destination,
            tags,
        };

        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account
            .streams_mut(&self.region)
            .insert(name.clone(), stream);

        let mut attributes = BTreeMap::new();
        attributes.insert("Arn".to_string(), arn.clone());
        attributes.insert("DeliveryStreamName".to_string(), name.clone());

        Ok(ProvisionResult {
            physical_id: name,
            attributes,
        })
    }

    pub(super) fn delete_firehose_delivery_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account.streams_mut(&self.region).remove(physical_id);
        Ok(())
    }
}
