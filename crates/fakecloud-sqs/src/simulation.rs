use chrono::Utc;

use crate::state::SharedSqsState;

/// Run the expiration processor across all queues.
///
/// Iterates every queue and removes messages (from both the pending and
/// in-flight collections) whose age exceeds the queue's
/// `MessageRetentionPeriod` attribute. Returns the total number of
/// expired messages removed.
pub fn tick_expiration(state: &SharedSqsState) -> u64 {
    let mut total = 0u64;
    let now = Utc::now();

    let mut mas = state.write();
    for (_, acct_state) in mas.iter_mut() {
        for queue in acct_state.queues.values_mut() {
            let retention_seconds: i64 = queue
                .attributes
                .get("MessageRetentionPeriod")
                .and_then(|s| s.parse().ok())
                .unwrap_or(345600); // default 4 days

            let before = queue.messages.len() + queue.inflight.len();

            queue
                .messages
                .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);
            queue
                .inflight
                .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);

            let after = queue.messages.len() + queue.inflight.len();
            total += (before - after) as u64;
        }
    } // end per-account loop

    total
}

/// Force-move messages that have exceeded `maxReceiveCount` to the DLQ.
///
/// Looks up the queue by name, checks its redrive policy, then moves any
/// messages (pending or in-flight) whose `receive_count` exceeds
/// `maxReceiveCount` to the configured dead-letter queue. Returns the
/// number of messages moved.
///
/// If the queue has no redrive policy, or the DLQ does not exist, this
/// is a no-op returning 0.
pub fn force_dlq(state: &SharedSqsState, queue_name: &str) -> u64 {
    let mut mas = state.write();

    // Find which account owns this queue by name
    let account_id = match mas.find_account(|s| s.name_to_url.contains_key(queue_name)) {
        Some(id) => id.to_string(),
        None => return 0,
    };

    let state = mas.get_or_create(&account_id);

    // Resolve queue URL from name
    let queue_url = match state.name_to_url.get(queue_name) {
        Some(url) => url.clone(),
        None => return 0,
    };

    // Extract redrive policy before mutating
    let redrive = match state.queues.get(&queue_url) {
        Some(q) => match &q.redrive_policy {
            Some(rp) => (rp.dead_letter_target_arn.clone(), rp.max_receive_count),
            None => return 0,
        },
        None => return 0,
    };

    let (dlq_arn, max_receive_count) = redrive;

    // Check that the DLQ exists
    let dlq_url = match state.queues.values().find(|q| q.arn == dlq_arn) {
        Some(q) => q.queue_url.clone(),
        None => return 0,
    };

    // Collect messages to move from pending queue
    let queue = state.queues.get_mut(&queue_url).unwrap();
    let mut to_move = Vec::new();

    // force-dlq intentionally uses >= (not >) to move messages at the threshold,
    // since the purpose is to force DLQ delivery without waiting for another receive
    let mut remaining = std::collections::VecDeque::new();
    while let Some(msg) = queue.messages.pop_front() {
        if msg.receive_count >= max_receive_count {
            to_move.push(msg);
        } else {
            remaining.push_back(msg);
        }
    }
    queue.messages = remaining;

    // Also check in-flight messages
    let mut remaining_inflight = Vec::new();
    for msg in queue.inflight.drain(..) {
        if msg.receive_count >= max_receive_count {
            to_move.push(msg);
        } else {
            remaining_inflight.push(msg);
        }
    }
    queue.inflight = remaining_inflight;

    let moved = to_move.len() as u64;

    // Move to DLQ
    let dlq = state.queues.get_mut(&dlq_url).unwrap();
    for mut msg in to_move {
        // Reset visibility and receipt handle when moving to DLQ
        msg.visible_at = None;
        msg.receipt_handle = None;
        dlq.messages.push_back(msg);
    }

    moved
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{RedrivePolicy, SqsMessage, SqsQueue, SqsState};
    use chrono::Duration;
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Arc;

    fn make_state() -> SharedSqsState {
        Arc::new(RwLock::new(MultiAccountState::<SqsState>::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )))
    }

    fn make_message(body: &str, age_seconds: i64, receive_count: u32) -> SqsMessage {
        SqsMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            receipt_handle: None,
            body: body.to_string(),
            md5_of_body: String::new(),
            sent_timestamp: 0,
            attributes: BTreeMap::new(),
            message_attributes: BTreeMap::new(),
            visible_at: None,
            receive_count,
            message_group_id: None,
            message_dedup_id: None,
            created_at: Utc::now() - Duration::seconds(age_seconds),
            sequence_number: None,
        }
    }

    fn add_queue(state: &SharedSqsState, name: &str, retention: Option<&str>) -> String {
        let mut accts = state.write();
        let s = accts.default_mut();
        let url = format!("http://localhost:4566/123456789012/{name}");
        let arn = Arn::new("sqs", "us-east-1", "123456789012", name).to_string();
        let mut attrs = BTreeMap::new();
        if let Some(r) = retention {
            attrs.insert("MessageRetentionPeriod".to_string(), r.to_string());
        }
        let queue = SqsQueue {
            queue_name: name.to_string(),
            queue_url: url.clone(),
            arn,
            created_at: Utc::now(),
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes: attrs,
            is_fifo: false,
            dedup_cache: BTreeMap::new(),
            redrive_policy: None,
            tags: BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: BTreeMap::new(),
            receive_attempt_cache: BTreeMap::new(),
        };
        s.queues.insert(url.clone(), queue);
        s.name_to_url.insert(name.to_string(), url.clone());
        url
    }

    #[test]
    fn expiration_removes_old_messages() {
        let state = make_state();
        let url = add_queue(&state, "test-q", Some("60")); // 60s retention

        {
            let mut accts = state.write();
            let q = accts.default_mut().queues.get_mut(&url).unwrap();
            q.messages.push_back(make_message("old", 120, 0)); // 120s old > 60s retention
        }

        let expired = tick_expiration(&state);
        assert_eq!(expired, 1);

        let accts = state.read();
        assert_eq!(accts.default_ref().queues[&url].messages.len(), 0);
    }

    #[test]
    fn expiration_keeps_young_messages() {
        let state = make_state();
        let url = add_queue(&state, "test-q", Some("60")); // 60s retention

        {
            let mut accts = state.write();
            let q = accts.default_mut().queues.get_mut(&url).unwrap();
            q.messages.push_back(make_message("young", 10, 0)); // 10s old < 60s retention
        }

        let expired = tick_expiration(&state);
        assert_eq!(expired, 0);

        let accts = state.read();
        assert_eq!(accts.default_ref().queues[&url].messages.len(), 1);
    }

    #[test]
    fn force_dlq_moves_over_max_receive_count() {
        let state = make_state();
        let dlq_url = add_queue(&state, "my-dlq", None);
        let src_url = add_queue(&state, "src-q", None);

        let dlq_arn = {
            let accts = state.read();
            accts.default_ref().queues[&dlq_url].arn.clone()
        };

        // Set redrive policy on source
        {
            let mut accts = state.write();
            let q = accts.default_mut().queues.get_mut(&src_url).unwrap();
            q.redrive_policy = Some(RedrivePolicy {
                dead_letter_target_arn: dlq_arn,
                max_receive_count: 2,
            });
            // Message received 3 times (>= maxReceiveCount of 2)
            q.messages.push_back(make_message("over", 5, 3));
        }

        let moved = force_dlq(&state, "src-q");
        assert_eq!(moved, 1);

        let accts = state.read();
        let s = accts.default_ref();
        assert_eq!(s.queues[&src_url].messages.len(), 0);
        assert_eq!(s.queues[&dlq_url].messages.len(), 1);
        assert_eq!(s.queues[&dlq_url].messages[0].body, "over");
    }

    #[test]
    fn force_dlq_keeps_under_max_receive_count() {
        let state = make_state();
        let dlq_url = add_queue(&state, "my-dlq", None);
        let src_url = add_queue(&state, "src-q", None);

        let dlq_arn = {
            let accts = state.read();
            accts.default_ref().queues[&dlq_url].arn.clone()
        };

        {
            let mut accts = state.write();
            let q = accts.default_mut().queues.get_mut(&src_url).unwrap();
            q.redrive_policy = Some(RedrivePolicy {
                dead_letter_target_arn: dlq_arn,
                max_receive_count: 3,
            });
            // Message received 1 time (< maxReceiveCount of 3)
            q.messages.push_back(make_message("under", 5, 1));
        }

        let moved = force_dlq(&state, "src-q");
        assert_eq!(moved, 0);

        let accts = state.read();
        let s = accts.default_ref();
        assert_eq!(s.queues[&src_url].messages.len(), 1);
        assert_eq!(s.queues[&dlq_url].messages.len(), 0);
    }

    #[test]
    fn force_dlq_no_redrive_policy_is_noop() {
        let state = make_state();
        add_queue(&state, "no-policy-q", None);

        let moved = force_dlq(&state, "no-policy-q");
        assert_eq!(moved, 0);
    }
}
