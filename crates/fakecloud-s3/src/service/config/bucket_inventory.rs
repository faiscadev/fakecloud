//! `S3Service` `inventory` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    pub(crate) fn put_bucket_inventory(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        // Use the Id from the XML body if available, otherwise fall back to query param
        let inv_id = extract_xml_value(&body_str, "Id")
            .or_else(|| req.query_params.get("id").cloned())
            .unwrap_or_default();
        let payload = {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.inventory_configs.insert(inv_id.clone(), body_str);
            let snap = InventorySnapshot {
                configs: b.inventory_configs.clone(),
            };
            toml::to_string(&snap).unwrap_or_default()
        };
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Inventory, &payload)
            .map_err(crate::service::persistence_error)?;
        // Real S3 delivers inventory reports on a daily/weekly schedule, not at
        // config time, so a bucket created and destroyed within a test never
        // accumulates a report object. Only generate it eagerly when opted in.
        if crate::logging::eager_delivery_enabled() {
            inventory::generate_inventory_report(&self.state, bucket, &inv_id);
        }
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_inventory(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let inv_id = req.query_params.get("id").cloned().unwrap_or_default();
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match b.inventory_configs.get(&inv_id) {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchConfiguration",
                format!("The specified configuration does not exist: {inv_id}"),
            )),
        }
    }

    pub(crate) fn list_bucket_inventory_configurations(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut body = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListInventoryConfigurationsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <IsTruncated>false</IsTruncated>",
        );
        let mut sorted_keys: Vec<_> = b.inventory_configs.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            if let Some(config) = b.inventory_configs.get(key) {
                body.push_str(config);
            }
        }
        body.push_str("</ListInventoryConfigurationsResult>");
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(crate) fn delete_bucket_inventory(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let inv_id = req.query_params.get("id").cloned().unwrap_or_default();
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.inventory_configs.remove(&inv_id);
        if b.inventory_configs.is_empty() {
            self.store
                .delete_bucket_subresource(bucket, BucketSubresource::Inventory)
                .map_err(crate::service::persistence_error)?;
        } else {
            let snap = InventorySnapshot {
                configs: b.inventory_configs.clone(),
            };
            let payload = toml::to_string(&snap).unwrap_or_default();
            self.store
                .put_bucket_subresource(bucket, BucketSubresource::Inventory, &payload)
                .map_err(crate::service::persistence_error)?;
        }
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
