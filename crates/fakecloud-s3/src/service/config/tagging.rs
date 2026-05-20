//! `S3Service` `tagging` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    pub(crate) fn get_bucket_tagging(
        &self,
        account_id: &str,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if b.tags.is_empty() {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchTagSet",
                "The TagSet does not exist",
                vec![("BucketName".to_string(), b.name.clone())],
            ));
        }
        let mut tags_xml = String::new();
        for (k, v) in &b.tags {
            tags_xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(k),
                xml_escape(v),
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <TagSet>{tags_xml}</TagSet></Tagging>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(crate) fn put_bucket_tagging(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_tagging_xml(body_str);

        // Validate tags: no duplicate keys
        validate_tags(&tags)?;

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.tags = tags.into_iter().collect();
        let snap = TagsSnapshot {
            tags: b.tags.clone(),
        };
        let payload = toml::to_string(&snap).unwrap_or_default();
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Tags, &payload)
            .map_err(crate::service::persistence_error)?;
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }

    pub(crate) fn delete_bucket_tagging(
        &self,
        account_id: &str,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.tags.clear();
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::Tags)
            .map_err(crate::service::persistence_error)?;
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }
}
