use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Datelike, Timelike, Utc};
use serde_json::json;

use fakecloud_core::delivery::DeliveryBus;

use crate::state::SharedEventBridgeState;

/// Parsed schedule expression.
enum Schedule {
    /// Rate-based: fire every `interval` duration.
    Rate(Duration),
    /// Cron-based: `cron(min hour dom month dow year)`.
    Cron(CronExpr),
}

/// A simplified cron expression with 6 fields: min hour dom month dow year.
/// Each field is either `Any` (wildcard) or a specific numeric value.
struct CronExpr {
    minute: CronField,
    hour: CronField,
    day_of_month: CronField,
    month: CronField,
    day_of_week: CronField,
    // year is parsed but not checked (always matches)
}

enum CronField {
    Any,
    Value(u32),
}

fn parse_schedule(expr: &str) -> Option<Schedule> {
    let expr = expr.trim();
    if let Some(inner) = expr.strip_prefix("rate(").and_then(|s| s.strip_suffix(')')) {
        return parse_rate(inner.trim());
    }
    if let Some(inner) = expr.strip_prefix("cron(").and_then(|s| s.strip_suffix(')')) {
        return parse_cron(inner.trim());
    }
    None
}

fn parse_rate(inner: &str) -> Option<Schedule> {
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }
    let value: u64 = parts[0].parse().ok()?;
    let unit = parts[1];
    let secs = match unit {
        "second" | "seconds" => value,
        "minute" | "minutes" => value * 60,
        "hour" | "hours" => value * 3600,
        "day" | "days" => value * 86400,
        _ => return None,
    };
    Some(Schedule::Rate(Duration::from_secs(secs)))
}

fn parse_cron(inner: &str) -> Option<Schedule> {
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 6 {
        return None;
    }
    Some(Schedule::Cron(CronExpr {
        minute: parse_cron_field(parts[0]),
        hour: parse_cron_field(parts[1]),
        day_of_month: parse_cron_field(parts[2]),
        month: parse_cron_field(parts[3]),
        day_of_week: parse_cron_field(parts[4]),
        // year field parsed but not stored (always matches)
    }))
}

fn parse_cron_field(s: &str) -> CronField {
    if s == "*" || s == "?" {
        return CronField::Any;
    }
    match s.parse::<u32>() {
        Ok(v) => CronField::Value(v),
        Err(_) => CronField::Any,
    }
}

fn cron_matches_now(cron: &CronExpr) -> bool {
    let now = Utc::now();
    let matches_field = |field: &CronField, actual: u32| -> bool {
        match field {
            CronField::Any => true,
            CronField::Value(v) => *v == actual,
        }
    };
    matches_field(&cron.minute, now.minute())
        && matches_field(&cron.hour, now.hour())
        && matches_field(&cron.day_of_month, now.day())
        && matches_field(&cron.month, now.month())
        && matches_field(&cron.day_of_week, now.weekday().num_days_from_sunday())
}

/// Background scheduler that fires scheduled EventBridge rules.
pub struct Scheduler {
    state: SharedEventBridgeState,
    delivery: Arc<DeliveryBus>,
}

impl Scheduler {
    pub fn new(state: SharedEventBridgeState, delivery: Arc<DeliveryBus>) -> Self {
        Self { state, delivery }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        // Track last-fired-minute for cron to avoid firing multiple times in the same minute
        let mut cron_last_minute: HashMap<String, (u32, u32)> = HashMap::new();

        loop {
            interval.tick().await;
            self.tick(&mut cron_last_minute);
        }
    }

    fn tick(&self, cron_last_minute: &mut HashMap<String, (u32, u32)>) {
        let now = Utc::now();

        // Collect rules that need to fire (to avoid holding lock during delivery)
        let mut to_fire: Vec<(String, Vec<crate::state::EventTarget>)> = Vec::new();

        {
            let mut state = self.state.write();
            let rule_keys: Vec<crate::state::RuleKey> = state.rules.keys().cloned().collect();

            for key in rule_keys {
                let rule = match state.rules.get(&key) {
                    Some(r) => r,
                    None => continue,
                };
                let name = rule.name.clone();

                if rule.state != "ENABLED" {
                    continue;
                }

                let schedule_expr = match &rule.schedule_expression {
                    Some(s) => s.clone(),
                    None => continue,
                };

                if rule.targets.is_empty() {
                    continue;
                }

                let schedule = match parse_schedule(&schedule_expr) {
                    Some(s) => s,
                    None => continue,
                };

                let should_fire = match &schedule {
                    Schedule::Rate(duration) => match rule.last_fired {
                        Some(last) => {
                            let elapsed = now.signed_duration_since(last);
                            elapsed.to_std().unwrap_or(Duration::ZERO) >= *duration
                        }
                        None => true, // Never fired, fire immediately
                    },
                    Schedule::Cron(cron) => {
                        if !cron_matches_now(cron) {
                            false
                        } else {
                            // Avoid firing multiple times in the same minute
                            let current = (now.hour(), now.minute());
                            let last = cron_last_minute.get(&name);
                            if last == Some(&current) {
                                false
                            } else {
                                cron_last_minute.insert(name.clone(), current);
                                true
                            }
                        }
                    }
                };

                if should_fire {
                    let targets = rule.targets.clone();
                    // Update last_fired while we hold the write lock
                    if let Some(r) = state.rules.get_mut(&key) {
                        r.last_fired = Some(now);
                    }
                    to_fire.push((name, targets));
                }
            }
        }
        // Lock is dropped here

        // Deliver events
        for (rule_name, targets) in to_fire {
            let event_id = uuid::Uuid::new_v4().to_string();
            let event_json = json!({
                "version": "0",
                "id": event_id,
                "source": "aws.events",
                "detail-type": "Scheduled Event",
                "detail": {},
                "time": now.to_rfc3339(),
                "region": "us-east-1",
            });
            let event_str = event_json.to_string();

            tracing::debug!(rule = %rule_name, targets = targets.len(), "scheduler firing");

            for target in &targets {
                let arn = &target.arn;
                if arn.contains(":sqs:") {
                    self.delivery.send_to_sqs(arn, &event_str, &HashMap::new());
                } else if arn.contains(":sns:") {
                    self.delivery
                        .publish_to_sns(arn, &event_str, Some("Scheduled Event"));
                } else if arn.contains(":lambda:") {
                    tracing::info!(
                        function_arn = %arn,
                        payload = %event_str,
                        "Scheduler delivering to Lambda function (stub)"
                    );
                    let mut state = self.state.write();
                    state
                        .lambda_invocations
                        .push(crate::state::LambdaInvocation {
                            function_arn: arn.clone(),
                            payload: event_str.clone(),
                            timestamp: now,
                        });
                } else if arn.contains(":logs:") {
                    tracing::info!(
                        log_group_arn = %arn,
                        payload = %event_str,
                        "Scheduler delivering to CloudWatch Logs (stub)"
                    );
                    let mut state = self.state.write();
                    state.log_deliveries.push(crate::state::LogDelivery {
                        log_group_arn: arn.clone(),
                        payload: event_str.clone(),
                        timestamp: now,
                    });
                } else if arn.contains(":states:") {
                    tracing::info!(
                        state_machine_arn = %arn,
                        payload = %event_str,
                        "Scheduler delivering to Step Functions (stub)"
                    );
                    let mut state = self.state.write();
                    state
                        .step_function_executions
                        .push(crate::state::StepFunctionExecution {
                            state_machine_arn: arn.clone(),
                            payload: event_str.clone(),
                            timestamp: now,
                        });
                } else if arn.starts_with("https://") || arn.starts_with("http://") {
                    let url = arn.clone();
                    let payload = event_str.clone();
                    tokio::spawn(async move {
                        let client = reqwest::Client::new();
                        let result = client
                            .post(&url)
                            .header("Content-Type", "application/json")
                            .body(payload)
                            .send()
                            .await;
                        if let Err(e) = result {
                            tracing::warn!(
                                endpoint = %url,
                                error = %e,
                                "Scheduler HTTP target delivery failed"
                            );
                        }
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rate_minutes() {
        let s = parse_schedule("rate(5 minutes)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(300)));
    }

    #[test]
    fn parse_rate_singular() {
        let s = parse_schedule("rate(1 hour)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(3600)));
    }

    #[test]
    fn parse_rate_seconds() {
        let s = parse_schedule("rate(1 second)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(1)));
    }

    #[test]
    fn parse_rate_days() {
        let s = parse_schedule("rate(2 days)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(172800)));
    }

    #[test]
    fn parse_cron_all_wildcards() {
        let s = parse_schedule("cron(* * * * ? *)");
        assert!(matches!(s, Some(Schedule::Cron(_))));
    }

    #[test]
    fn parse_cron_specific_values() {
        let s = parse_schedule("cron(0 12 * * ? *)");
        match s {
            Some(Schedule::Cron(c)) => {
                assert!(matches!(c.minute, CronField::Value(0)));
                assert!(matches!(c.hour, CronField::Value(12)));
                assert!(matches!(c.day_of_month, CronField::Any));
                assert!(matches!(c.month, CronField::Any));
                assert!(matches!(c.day_of_week, CronField::Any));
            }
            _ => panic!("expected cron"),
        }
    }

    #[test]
    fn parse_invalid_returns_none() {
        assert!(parse_schedule("invalid").is_none());
        assert!(parse_schedule("rate()").is_none());
        assert!(parse_schedule("rate(abc minutes)").is_none());
        assert!(parse_schedule("cron(1 2 3)").is_none());
    }
}
