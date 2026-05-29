//! Resource tagging: TagResource / UntagResource / ListTagsForResource.
//! Tags are keyed by resource ARN, account-global.

use fakecloud_core::query::required_query_param;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    collect_member_values, parse_tags, validate_len, xml_escape, xml_response, CloudWatchService,
};

impl CloudWatchService {
    pub(crate) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Tags is required, but an empty list serialises to zero wire params
        // (indistinguishable from omission), and the op declares no
        // missing-parameter error — so an empty tag set is a no-op rather than
        // an undeclared 4xx.
        validate_len(req, "ResourceARN", 1, 1024)?;
        let arn = required_query_param(req, "ResourceARN")?;
        let tags = parse_tags(req, "Tags");
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let bucket = acct.tags.entry(arn).or_default();
        for (k, v) in tags {
            bucket.insert(k, v);
        }
        Ok(xml_response("TagResource", "", &req.request_id))
    }

    pub(crate) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "ResourceARN", 1, 1024)?;
        let arn = required_query_param(req, "ResourceARN")?;
        let keys = collect_member_values(req, "TagKeys");
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        if let Some(bucket) = acct.tags.get_mut(&arn) {
            for k in &keys {
                bucket.remove(k);
            }
        }
        Ok(xml_response("UntagResource", "", &req.request_id))
    }

    pub(crate) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "ResourceARN", 1, 1024)?;
        let arn = required_query_param(req, "ResourceARN")?;
        let state = self.state.read();
        let mut inner = String::from("<Tags>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(bucket) = acct.tags.get(&arn) {
                for (k, v) in bucket {
                    inner.push_str("<member>");
                    inner.push_str(&format!("<Key>{}</Key>", xml_escape(k)));
                    inner.push_str(&format!("<Value>{}</Value>", xml_escape(v)));
                    inner.push_str("</member>");
                }
            }
        }
        inner.push_str("</Tags>");
        Ok(xml_response("ListTagsForResource", &inner, &req.request_id))
    }
}
