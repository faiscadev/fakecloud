//! S3 Control resource tagging: `TagResource`, `UntagResource` and
//! `ListTagsForResource` on `/v20180820/tags/{resourceArn+}`.
//!
//! A general purpose bucket ARN (`arn:<partition>:s3:::<bucket>`) shares its
//! tag set with `PutBucketTagging` / `GetBucketTagging`. An access point ARN
//! (`arn:<partition>:s3:<region>:<account>:accesspoint/<name>`) carries its
//! own tag set.

use std::collections::BTreeMap;

use bytes::Bytes;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use fakecloud_persistence::{BucketSubresource, TagsSnapshot};

use crate::state::S3State;

use super::{no_such_bucket, s3_xml, validate_tags, xml_escape, S3Service};

/// Most tags an S3 resource can carry through S3 Control.
const MAX_TAGS: usize = 50;

/// The S3 resource an S3 Control tagging ARN names.
#[derive(Debug, PartialEq, Eq)]
enum TaggedResource {
    Bucket(String),
    AccessPoint(String),
}

fn invalid_arn(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        format!("Invalid resource ARN: {arn}"),
    )
}

fn no_such_access_point(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchAccessPoint",
        format!("The specified accesspoint does not exist: {name}"),
    )
}

/// Parse the resource ARN of an S3 Control tagging request. Only the S3
/// resources fakecloud models are accepted; any other ARN is rejected.
fn parse_resource_arn(arn: &str, account_id: &str) -> Result<TaggedResource, AwsServiceError> {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let [prefix, partition, service, region, account, resource] = parts[..] else {
        return Err(invalid_arn(arn));
    };
    if prefix != "arn" || !partition.starts_with("aws") || service != "s3" {
        return Err(invalid_arn(arn));
    }
    if region.is_empty() && account.is_empty() {
        if resource.is_empty() || resource.contains('/') {
            return Err(invalid_arn(arn));
        }
        return Ok(TaggedResource::Bucket(resource.to_string()));
    }
    match resource.split_once('/') {
        Some(("accesspoint", name)) if !region.is_empty() && !name.is_empty() => {
            if account != account_id {
                return Err(no_such_access_point(name));
            }
            Ok(TaggedResource::AccessPoint(name.to_string()))
        }
        _ => Err(invalid_arn(arn)),
    }
}

/// The resource ARN from the raw request path, decoded once. The label is
/// greedy, so an access point ARN keeps its `/`.
pub(super) fn resource_arn_from_path(req: &AwsRequest) -> Option<String> {
    let raw = req.raw_path.strip_prefix("/v20180820/tags/")?;
    let arn = percent_encoding::percent_decode_str(raw)
        .decode_utf8_lossy()
        .into_owned();
    (!arn.is_empty()).then_some(arn)
}

/// Whether `req` is an S3 Control `/v20180820/tags/{resourceArn}` request.
pub(super) fn is_control_tags_request(req: &AwsRequest) -> bool {
    fakecloud_core::protocol::is_s3_control_host(&req.headers)
        && resource_arn_from_path(req).is_some()
}

/// Every `tagKeys` value in the raw query string, in order. The parsed
/// query map keeps only the last of a repeated key.
pub(super) fn tag_keys_from_query(raw_query: &str) -> Vec<String> {
    raw_query
        .split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (k == "tagKeys").then(|| {
                percent_encoding::percent_decode_str(&v.replace('+', " "))
                    .decode_utf8_lossy()
                    .into_owned()
            })
        })
        .collect()
}

fn no_content() -> AwsResponse {
    AwsResponse {
        status: StatusCode::NO_CONTENT,
        content_type: "application/xml".to_string(),
        body: Bytes::new().into(),
        headers: HeaderMap::new(),
    }
}

impl S3Service {
    /// Dispatch an S3 Control `/v20180820/tags/...` request.
    pub(super) fn handle_control_tags(
        &self,
        account_id: &str,
        req: &AwsRequest,
    ) -> Option<Result<AwsResponse, AwsServiceError>> {
        let op = match req.method {
            http::Method::GET => Self::list_tags_for_resource,
            http::Method::POST => Self::tag_resource,
            http::Method::DELETE => Self::untag_resource,
            _ => return None,
        };
        Some(match resource_arn_from_path(req) {
            Some(arn) => op(self, account_id, req, &arn),
            None => Err(invalid_arn("")),
        })
    }

    fn list_tags_for_resource(
        &self,
        account_id: &str,
        _req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource = parse_resource_arn(arn, account_id)?;
        let accts = self.state.read();
        let empty = S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&empty);
        let tags = match &resource {
            TaggedResource::Bucket(bucket) => {
                &state
                    .buckets
                    .get(bucket)
                    .ok_or_else(|| no_such_bucket(bucket))?
                    .tags
            }
            TaggedResource::AccessPoint(name) => {
                &state
                    .access_points
                    .get(name)
                    .ok_or_else(|| no_such_access_point(name))?
                    .tags
            }
        };
        let mut tags_xml = String::new();
        for (k, v) in tags {
            tags_xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(k),
                xml_escape(v),
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListTagsForResourceResult xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">\
             <Tags>{tags_xml}</Tags></ListTagsForResourceResult>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn tag_resource(
        &self,
        account_id: &str,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource = parse_resource_arn(arn, account_id)?;
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let new_tags = super::tags::read_tag_set(body_str, "TagResourceRequest", "Tags")?;
        validate_tags(&new_tags)?;

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let tags = resource_tags_mut(state, &resource)?;
        let mut merged = tags.clone();
        merged.extend(new_tags);
        if merged.len() > MAX_TAGS {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidTag",
                format!("The resource cannot have more than {MAX_TAGS} tags"),
            ));
        }
        *tags = merged;
        if let TaggedResource::Bucket(bucket) = &resource {
            self.persist_bucket_tags(bucket, tags)?;
        }
        Ok(no_content())
    }

    fn untag_resource(
        &self,
        account_id: &str,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource = parse_resource_arn(arn, account_id)?;
        let keys = tag_keys_from_query(&req.raw_query);

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let tags = resource_tags_mut(state, &resource)?;
        for key in &keys {
            tags.remove(key);
        }
        if let TaggedResource::Bucket(bucket) = &resource {
            self.persist_bucket_tags(bucket, tags)?;
        }
        Ok(no_content())
    }

    /// Persist a bucket's tag set the way `PutBucketTagging` /
    /// `DeleteBucketTagging` do, so both APIs survive a restart alike.
    fn persist_bucket_tags(
        &self,
        bucket: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), AwsServiceError> {
        if tags.is_empty() {
            return self
                .store
                .delete_bucket_subresource(bucket, BucketSubresource::Tags)
                .map_err(super::persistence_error);
        }
        let snap = TagsSnapshot { tags: tags.clone() };
        let payload = super::toml_or_internal_error(&snap)?;
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Tags, &payload)
            .map_err(super::persistence_error)
    }
}

fn resource_tags_mut<'a>(
    state: &'a mut S3State,
    resource: &TaggedResource,
) -> Result<&'a mut BTreeMap<String, String>, AwsServiceError> {
    Ok(match resource {
        TaggedResource::Bucket(bucket) => {
            &mut state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?
                .tags
        }
        TaggedResource::AccessPoint(name) => {
            &mut state
                .access_points
                .get_mut(name)
                .ok_or_else(|| no_such_access_point(name))?
                .tags
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bucket_and_access_point_arns() {
        assert_eq!(
            parse_resource_arn("arn:aws:s3:::my-bucket", "123456789012").unwrap(),
            TaggedResource::Bucket("my-bucket".to_string())
        );
        assert_eq!(
            parse_resource_arn(
                "arn:aws:s3:us-east-1:123456789012:accesspoint/ap",
                "123456789012"
            )
            .unwrap(),
            TaggedResource::AccessPoint("ap".to_string())
        );
        for bad in [
            "not-an-arn",
            "arn:aws:sqs:::q",
            "arn:aws:s3:::",
            "arn:aws:s3:::b/k",
            "arn:aws:s3:us-east-1:123456789012:storage-lens/x",
        ] {
            assert!(parse_resource_arn(bad, "123456789012").is_err(), "{bad}");
        }
    }

    #[test]
    fn collects_repeated_tag_keys() {
        assert_eq!(
            tag_keys_from_query("tagKeys=a&tagKeys=b%20c&other=x&tagKeys=d+e"),
            vec!["a", "b c", "d e"]
        );
    }
}
