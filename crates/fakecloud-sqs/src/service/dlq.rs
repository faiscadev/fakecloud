//! `SqsService` `dlq` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    pub(super) fn list_dead_letter_source_queues(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
        let queue_arn = queue.arn.clone();

        // Find all queues whose redrive policy targets this queue
        let source_urls: Vec<String> = state
            .queues
            .values()
            .filter(|q| {
                q.redrive_policy
                    .as_ref()
                    .map(|rp| rp.dead_letter_target_arn == queue_arn)
                    .unwrap_or(false)
            })
            .map(|q| q.queue_url.clone())
            .collect();

        Ok(sqs_response(
            "ListDeadLetterSourceQueues",
            json!({ "queueUrls": source_urls }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }
}
