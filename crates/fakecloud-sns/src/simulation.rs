use crate::state::SharedSnsState;

/// A pending subscription confirmation.
#[derive(Debug, Clone)]
pub struct PendingConfirmation {
    pub subscription_arn: String,
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
}

/// List all subscriptions that are pending confirmation.
pub fn list_pending_confirmations(state: &SharedSnsState) -> Vec<PendingConfirmation> {
    let s = state.read();
    s.subscriptions
        .values()
        .filter(|sub| !sub.confirmed)
        .map(|sub| PendingConfirmation {
            subscription_arn: sub.subscription_arn.clone(),
            topic_arn: sub.topic_arn.clone(),
            protocol: sub.protocol.clone(),
            endpoint: sub.endpoint.clone(),
        })
        .collect()
}

/// Force-confirm a subscription by its ARN. Returns true if the
/// subscription was found and confirmed (or was already confirmed).
pub fn confirm_subscription(state: &SharedSnsState, subscription_arn: &str) -> bool {
    let mut s = state.write();
    match s.subscriptions.get_mut(subscription_arn) {
        Some(sub) => {
            sub.confirmed = true;
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SnsState;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedSnsState {
        Arc::new(RwLock::new(SnsState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )))
    }

    fn add_subscription(
        state: &SharedSnsState,
        topic_arn: &str,
        protocol: &str,
        endpoint: &str,
        confirmed: bool,
    ) -> String {
        let sub_arn = format!("{}:{}", topic_arn, uuid::Uuid::new_v4());
        let mut s = state.write();
        s.subscriptions.insert(
            sub_arn.clone(),
            crate::state::SnsSubscription {
                subscription_arn: sub_arn.clone(),
                topic_arn: topic_arn.to_string(),
                protocol: protocol.to_string(),
                endpoint: endpoint.to_string(),
                owner: "123456789012".to_string(),
                attributes: HashMap::new(),
                confirmed,
            },
        );
        sub_arn
    }

    #[test]
    fn list_pending_finds_unconfirmed() {
        let state = make_state();
        let topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic";
        add_subscription(&state, topic_arn, "http", "http://example.com/hook", false);
        add_subscription(
            &state,
            topic_arn,
            "sqs",
            "arn:aws:sqs:us-east-1:123456789012:q",
            true,
        );

        let pending = list_pending_confirmations(&state);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].protocol, "http");
        assert_eq!(pending[0].endpoint, "http://example.com/hook");
    }

    #[test]
    fn list_pending_returns_empty_when_all_confirmed() {
        let state = make_state();
        let topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic";
        add_subscription(
            &state,
            topic_arn,
            "sqs",
            "arn:aws:sqs:us-east-1:123456789012:q",
            true,
        );

        let pending = list_pending_confirmations(&state);
        assert!(pending.is_empty());
    }

    #[test]
    fn confirm_subscription_sets_confirmed() {
        let state = make_state();
        let topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic";
        let sub_arn = add_subscription(&state, topic_arn, "http", "http://example.com/hook", false);

        assert!(!state.read().subscriptions[&sub_arn].confirmed);

        let result = confirm_subscription(&state, &sub_arn);
        assert!(result);
        assert!(state.read().subscriptions[&sub_arn].confirmed);
    }

    #[test]
    fn confirm_subscription_returns_false_for_unknown() {
        let state = make_state();
        let result = confirm_subscription(&state, "arn:aws:sns:us-east-1:123456789012:nope:xxx");
        assert!(!result);
    }
}
