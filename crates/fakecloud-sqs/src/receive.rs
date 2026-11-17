//! Taking messages off a queue the way `ReceiveMessage` does, shared by the
//! SQS API and by the Lambda event source mapping poller so both consumers see
//! one in-flight set: a received message moves to `queue.inflight` with a
//! fresh receipt handle, its receive count goes up, and a FIFO message group
//! with a message in flight hands out nothing else until that one is deleted or
//! its visibility timeout runs out.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};

use crate::state::{SqsMessage, SqsQueue, SqsState};

/// Messages taken by [`SqsQueue::take_receivable`].
#[derive(Debug, Default)]
pub struct TakenMessages {
    /// The received messages, removed from `queue.messages` with their receipt
    /// handle, visibility deadline and receive count set, but not yet in
    /// `queue.inflight`: the caller adds them once it has opened their bodies
    /// ([`SqsQueue::commit_taken`]) or undoes the receive
    /// ([`SqsQueue::restore_taken`]).
    pub messages: Vec<SqsMessage>,
    /// Messages past the redrive policy's `maxReceiveCount`, with the
    /// dead-letter queue ARN they go to ([`SqsState::route_dead_letters`]).
    pub dead_letters: Vec<(String, SqsMessage)>,
}

impl SqsQueue {
    /// Take up to `max_messages` receivable messages, hidden for
    /// `visibility_timeout` seconds from `now`. First drops messages past the
    /// retention period and returns in-flight messages whose visibility
    /// timeout ran out to the queue.
    ///
    /// `held(message)` marks messages a consumer still has in hand even though
    /// their visibility timeout ran out (the event source mapping poller's
    /// batches with the function): they are not handed out again and, on a
    /// FIFO queue, lock their group like an in-flight message.
    pub fn take_receivable(
        &mut self,
        max_messages: usize,
        visibility_timeout: i64,
        now: DateTime<Utc>,
        held: &dyn Fn(&SqsMessage) -> bool,
    ) -> TakenMessages {
        let retention_seconds: i64 = self
            .attributes
            .get("MessageRetentionPeriod")
            .and_then(|s| s.parse().ok())
            .unwrap_or(345600); // default 4 days
        self.messages
            .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);
        self.inflight
            .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);

        // Return expired inflight messages. Once a message's visibility window
        // lapses its prior receipt handle is invalidated (real SQS at-least-
        // once semantics: a stale handle must not act on a message that may now
        // be redelivered to another consumer). Pruning the map entry also
        // bounds its growth under redelivery churn. A FIFO queue puts them back
        // at the front to keep their order.
        let mut returned = Vec::new();
        self.inflight.retain(|m| {
            if m.visible_at.is_some_and(|v| v <= now) {
                returned.push(m.clone());
                return false;
            }
            true
        });
        if self.is_fifo {
            for mut m in returned.into_iter().rev() {
                self.receipt_handle_map.remove(&m.message_id);
                m.receipt_handle = None;
                m.visible_at = None;
                self.messages.push_front(m);
            }
        } else {
            for mut m in returned {
                self.receipt_handle_map.remove(&m.message_id);
                m.receipt_handle = None;
                m.visible_at = None;
                self.messages.push_back(m);
            }
        }

        let redrive_policy = self.redrive_policy.clone();
        let mut taken = TakenMessages::default();
        let receive = |queue: &mut SqsQueue, mut msg: SqsMessage, taken: &mut TakenMessages| {
            msg.receive_count += 1;
            if msg.first_received_at.is_none() {
                msg.first_received_at = Some(Utc::now().timestamp_millis());
            }
            if let Some(ref rp) = redrive_policy {
                if msg.receive_count > rp.max_receive_count {
                    taken
                        .dead_letters
                        .push((rp.dead_letter_target_arn.clone(), msg));
                    return;
                }
            }
            let new_handle = uuid::Uuid::new_v4().to_string();
            queue
                .receipt_handle_map
                .entry(msg.message_id.clone())
                .or_default()
                .push(new_handle.clone());
            msg.receipt_handle = Some(new_handle);
            msg.visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
            taken.messages.push(msg);
        };

        let available = |msg: &SqsMessage| !msg.visible_at.is_some_and(|v| v > now) && !held(msg);

        if self.is_fifo {
            // FIFO: deliver in order, skipping groups with a message in
            // flight (or held). Several messages of one group can go out in
            // one batch.
            let locked_groups: HashSet<String> = self
                .inflight
                .iter()
                .chain(self.messages.iter().filter(|m| held(m)))
                .filter_map(|m| m.message_group_id.clone())
                .collect();
            let mut remaining = VecDeque::new();
            while let Some(msg) = self.messages.pop_front() {
                let group = msg.message_group_id.as_deref().unwrap_or("");
                if !available(&msg) || locked_groups.contains(group) {
                    remaining.push_back(msg);
                    continue;
                }
                if taken.messages.len() < max_messages {
                    receive(self, msg, &mut taken);
                } else {
                    remaining.push_back(msg);
                    break;
                }
            }
            remaining.extend(self.messages.drain(..));
            self.messages = remaining;
        } else {
            // Standard queue with Fair Queues support: when messages have a
            // MessageGroupId, groups with fewer in-flight messages go first
            // so a noisy neighbour cannot starve the others.
            let mut inflight_per_group: HashMap<String, usize> = HashMap::new();
            for m in &self.inflight {
                if let Some(ref group) = m.message_group_id {
                    *inflight_per_group.entry(group.clone()).or_default() += 1;
                }
            }
            let mut visible: Vec<SqsMessage> = Vec::new();
            let mut remaining = VecDeque::new();
            while let Some(msg) = self.messages.pop_front() {
                if available(&msg) {
                    visible.push(msg);
                } else {
                    remaining.push_back(msg);
                }
            }
            visible.sort_by_key(|m| {
                m.message_group_id
                    .as_ref()
                    .and_then(|g| inflight_per_group.get(g).copied())
                    .unwrap_or(0)
            });
            for msg in visible {
                if taken.messages.len() < max_messages {
                    receive(self, msg, &mut taken);
                } else {
                    remaining.push_back(msg);
                }
            }
            self.messages = remaining;
        }
        taken
    }

    /// Put messages [`Self::take_receivable`] took into flight.
    pub fn commit_taken(&mut self, messages: &[SqsMessage]) {
        self.inflight.extend(messages.iter().cloned());
    }

    /// Undo a receive of `messages` that never reached the consumer: they go
    /// back to the front of the queue, visible, with their receive count and
    /// the receipt handle the receive minted taken back.
    pub fn restore_taken(&mut self, messages: Vec<SqsMessage>) {
        for mut m in messages.into_iter().rev() {
            m.visible_at = None;
            m.receive_count = m.receive_count.saturating_sub(1);
            // An undone first receive was never a receive at all: it must
            // not stamp `ApproximateFirstReceiveTimestamp`.
            if m.receive_count == 0 {
                m.first_received_at = None;
            }
            if let Some(handle) = m.receipt_handle.take() {
                if let Some(list) = self.receipt_handle_map.get_mut(&m.message_id) {
                    list.retain(|h| h != &handle);
                    if list.is_empty() {
                        self.receipt_handle_map.remove(&m.message_id);
                    }
                }
            }
            self.messages.push_front(m);
        }
    }

    /// Delete the messages with these ids, wherever they are (in flight, or
    /// back in the queue after their visibility timeout ran out). Like
    /// `DeleteMessage`, the receipt handles stay known, so a consumer that also
    /// received the message can still delete it with its own handle.
    pub fn delete_messages(&mut self, message_ids: &[String]) {
        self.messages
            .retain(|m| !message_ids.contains(&m.message_id));
        self.inflight
            .retain(|m| !message_ids.contains(&m.message_id));
    }

    /// Undo the receive of `messages` that are still in flight (see
    /// [`Self::restore_taken`]); one deleted or purged meanwhile stays gone.
    pub fn restore_in_flight(&mut self, messages: Vec<SqsMessage>) {
        let mut restorable = Vec::new();
        for m in messages {
            if let Some(i) = self
                .inflight
                .iter()
                .position(|f| f.message_id == m.message_id)
            {
                self.inflight.remove(i);
                restorable.push(m);
            }
        }
        self.restore_taken(restorable);
    }
}

impl SqsState {
    /// Move messages past their source queue's `maxReceiveCount` to their
    /// dead-letter queue, with a fresh receive history (AWS resets
    /// `ApproximateReceiveCount` on the move). If the redrive target does not
    /// resolve to a known queue the message returns to the source queue
    /// instead of being dropped (also with its receive history reset, so the
    /// next receive does not re-redrive it into a hot loop): SQS never
    /// destroys a message over a misconfigured redrive target.
    pub fn route_dead_letters(
        &mut self,
        source_arn: &str,
        dead_letters: Vec<(String, SqsMessage)>,
    ) {
        for (dlq_arn, mut msg) in dead_letters {
            msg.receipt_handle = None;
            msg.visible_at = None;
            msg.receive_count = 0;
            msg.first_received_at = None;
            if let Some(dlq) = self.queues.values_mut().find(|q| q.arn == dlq_arn) {
                dlq.messages.push_back(msg);
            } else {
                tracing::warn!(
                    queue = %source_arn,
                    dlq_arn = %dlq_arn,
                    "redrive target queue not found; returning message to source queue"
                );
                if let Some(source) = self.queues.values_mut().find(|q| q.arn == source_arn) {
                    source.messages.push_back(msg);
                }
            }
        }
    }
}
