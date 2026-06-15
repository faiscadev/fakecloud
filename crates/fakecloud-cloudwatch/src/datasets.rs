//! CloudWatch dataset KMS key management.
//!
//! Datasets are referenced by an identifier and carry an optional
//! customer-managed KMS key. There is no Create API: a dataset entry is
//! materialized the first time `AssociateDatasetKmsKey` is called for an
//! identifier, and `GetDataset` on an unknown identifier returns
//! `ResourceNotFoundException`. `DisassociateDatasetKmsKey` clears the key
//! but leaves the dataset in place.

use uuid::Uuid;

use fakecloud_core::query::required_query_param;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    empty_metadata_response, not_found, validate_len, xml_escape, xml_response, CloudWatchService,
};
use crate::state::Dataset;

impl CloudWatchService {
    pub(crate) fn associate_dataset_kms_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "DatasetIdentifier", 1, 2048)?;
        validate_len(req, "KmsKeyArn", 1, 2048)?;
        let identifier = required_query_param(req, "DatasetIdentifier")?;
        let kms_key_arn = required_query_param(req, "KmsKeyArn")?;

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let datasets = acct.datasets_in_mut(&req.region);
        let entry = datasets
            .entry(identifier.clone())
            .or_insert_with(|| Dataset {
                id: Uuid::new_v4().to_string(),
                arn: format!(
                    "arn:aws:cloudwatch:{}:{}:dataset/{identifier}",
                    req.region, req.account_id
                ),
                kms_key_arn: None,
            });
        entry.kms_key_arn = Some(kms_key_arn);
        Ok(empty_metadata_response(
            "AssociateDatasetKmsKey",
            &req.request_id,
        ))
    }

    pub(crate) fn disassociate_dataset_kms_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "DatasetIdentifier", 1, 2048)?;
        let identifier = required_query_param(req, "DatasetIdentifier")?;

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let dataset = acct
            .datasets_in_mut(&req.region)
            .get_mut(&identifier)
            .ok_or_else(|| not_found(format!("Dataset {identifier} does not exist")))?;
        dataset.kms_key_arn = None;
        Ok(empty_metadata_response(
            "DisassociateDatasetKmsKey",
            &req.request_id,
        ))
    }

    pub(crate) fn get_dataset(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "DatasetIdentifier", 1, 2048)?;
        let identifier = required_query_param(req, "DatasetIdentifier")?;

        let state = self.state.read();
        let dataset = state
            .get(&req.account_id)
            .and_then(|a| a.datasets_in(&req.region))
            .and_then(|m| m.get(&identifier))
            .cloned()
            .ok_or_else(|| not_found(format!("Dataset {identifier} does not exist")))?;

        let mut inner = format!("<DatasetId>{}</DatasetId>", xml_escape(&dataset.id));
        inner.push_str(&format!("<Arn>{}</Arn>", xml_escape(&dataset.arn)));
        if let Some(key) = &dataset.kms_key_arn {
            inner.push_str(&format!("<KmsKeyArn>{}</KmsKeyArn>", xml_escape(key)));
        }
        Ok(xml_response("GetDataset", &inner, &req.request_id))
    }
}
