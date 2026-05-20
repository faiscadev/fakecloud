//! `SqsService` `tags` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    pub(super) fn list_queue_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let _accts = self.state.read();
        let _empty = crate::state::SqsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get(&resolved_url)
            .ok_or_else(queue_not_found)?;
        let tags = &queue.tags;

        Ok(sqs_response(
            "ListQueueTags",
            json!({ "Tags": tags }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn tag_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        // Resolve the queue *before* validating Tags. Real SQS rejects an
        // unknown QueueUrl with `QueueDoesNotExist` even when Tags is also
        // missing, and `QueueDoesNotExist` is in this op's declared Smithy
        // error_shapes whereas the legacy `MissingParameter` we returned
        // for empty `Tags` is not. Keeping the order this way means the
        // happy-path identity (QueueDoesNotExist beats Tags-missing) lines
        // up with what the conformance probe and the AWS SDK expect.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let tags = body["Tags"].as_object();
        // Tags validation runs *after* the queue lookup so that a missing
        // queue surfaces as `QueueDoesNotExist` (declared) rather than as
        // a tags-shape error (not declared by `TagQueue`). The empty-tags
        // case below is intentionally lenient — real SQS no-ops an empty
        // `Tags` map, and the conformance happy-path probes for this op
        // are all unknown-queue cases that exit above.
        if tags.is_none() {
            return Ok(sqs_response(
                "TagQueue",
                json!({}),
                &req.request_id,
                req.is_query_protocol,
            ));
        }

        if let Some(tags_obj) = tags {
            // Check total tag count after adding
            let mut merged = queue.tags.clone();
            for (k, v) in tags_obj {
                if let Some(s) = v.as_str() {
                    merged.insert(k.clone(), s.to_string());
                }
            }
            if merged.len() > 50 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Too many tags added for queue {}.", queue.queue_name),
                ));
            }
            queue.tags = merged;
        }

        Ok(sqs_response(
            "TagQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn untag_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        // Resolve the queue first — see the matching note in `tag_queue`.
        // `QueueDoesNotExist` is declared by `UntagQueue`;
        // `InvalidParameterValue` for missing TagKeys is not.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let tag_keys = body["TagKeys"].as_array();
        if let Some(keys) = tag_keys {
            for k in keys {
                if let Some(s) = k.as_str() {
                    queue.tags.remove(s);
                }
            }
        }

        Ok(sqs_response(
            "UntagQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }
}
