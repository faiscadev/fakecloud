use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{
    no_such_bucket, no_such_key, no_such_key_with_detail, parse_tagging_xml, s3_xml, xml_escape,
    S3Service,
};

impl S3Service {
    pub(super) fn get_object_tagging(
        &self,
        _req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        let mut tags_xml = String::new();
        for (k, v) in &obj.tags {
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

    pub(super) fn put_object_tagging(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_tagging_xml(body_str);

        // Validate: no aws: prefix
        for (k, _) in &tags {
            if k.starts_with("aws:") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidTag",
                    "System tags cannot be added/updated by requester",
                ));
            }
        }

        // Validate: max 10 tags
        if tags.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequest",
                "Object tags cannot be greater than 10",
            ));
        }

        let version_id = req.query_params.get("versionId").map(|s| s.to_string());

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut response_headers = HeaderMap::new();

        if let Some(ref vid) = version_id {
            // Version-specific tagging
            let mut found = false;

            // Check versioned objects
            if let Some(versions) = b.object_versions.get_mut(key) {
                if let Some(obj) = versions
                    .iter_mut()
                    .find(|o| o.version_id.as_deref() == Some(vid.as_str()))
                {
                    if obj.is_delete_marker {
                        return Err(AwsServiceError::aws_error_with_fields(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "MethodNotAllowed",
                            "The specified method is not allowed against this resource.",
                            vec![
                                ("Method".to_string(), "PUT".to_string()),
                                ("ResourceType".to_string(), "DeleteMarker".to_string()),
                            ],
                        ));
                    }
                    obj.tags = tags.clone().into_iter().collect();
                    response_headers.insert("x-amz-version-id", vid.parse().unwrap());
                    found = true;
                }
            }

            // Also check current object
            if !found {
                if let Some(obj) = b.objects.get_mut(key) {
                    if obj.version_id.as_deref() == Some(vid.as_str()) {
                        if obj.is_delete_marker {
                            return Err(AwsServiceError::aws_error_with_fields(
                                StatusCode::METHOD_NOT_ALLOWED,
                                "MethodNotAllowed",
                                "The specified method is not allowed against this resource.",
                                vec![
                                    ("Method".to_string(), "PUT".to_string()),
                                    ("ResourceType".to_string(), "DeleteMarker".to_string()),
                                ],
                            ));
                        }
                        obj.tags = tags.into_iter().collect();
                        response_headers.insert("x-amz-version-id", vid.parse().unwrap());
                        found = true;
                    }
                }
            }

            if !found {
                return Err(AwsServiceError::aws_error_with_fields(
                    StatusCode::NOT_FOUND,
                    "NoSuchVersion",
                    "The specified version does not exist.",
                    vec![
                        ("Key".to_string(), key.to_string()),
                        ("VersionId".to_string(), vid.to_string()),
                    ],
                ));
            }
        } else {
            let obj = b
                .objects
                .get_mut(key)
                .ok_or_else(|| no_such_key_with_detail(key))?;
            if obj.is_delete_marker {
                return Err(no_such_key_with_detail(key));
            }
            obj.tags = tags.into_iter().collect();
            if let Some(ref vid) = obj.version_id {
                response_headers.insert("x-amz-version-id", vid.parse().unwrap());
            }
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: response_headers,
        })
    }

    // ---- Multipart Upload ----

    pub(super) fn delete_object_tagging(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
        obj.tags.clear();
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }
}
