use std::time::Duration;

use chrono::{NaiveDate, Utc};

use crate::state::SharedS3State;

/// Background task that processes S3 lifecycle rules.
///
/// Every 60 seconds, iterates all buckets with lifecycle configurations,
/// parses the lifecycle XML, and:
/// - Deletes objects matching expiration rules (by Days or Date)
/// - Updates storage class for objects matching transition rules
pub struct LifecycleProcessor {
    state: SharedS3State,
}

impl LifecycleProcessor {
    pub fn new(state: SharedS3State) -> Self {
        Self { state }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;
            self.tick();
        }
    }

    fn tick(&self) {
        let now = Utc::now();
        let today = now.date_naive();

        // Collect bucket names and their lifecycle configs (to avoid holding lock during processing)
        let bucket_configs: Vec<(String, String)> = {
            let state = self.state.read();
            state
                .buckets
                .values()
                .filter_map(|b| {
                    b.lifecycle_config
                        .as_ref()
                        .map(|cfg| (b.name.clone(), cfg.clone()))
                })
                .collect()
        };

        for (bucket_name, config_xml) in bucket_configs {
            let rules = match parse_lifecycle_rules(&config_xml) {
                Some(r) => r,
                None => continue,
            };

            for rule in &rules {
                if rule.status != "Enabled" {
                    continue;
                }

                self.process_rule(&bucket_name, rule, today);
            }
        }
    }

    fn process_rule(&self, bucket_name: &str, rule: &LifecycleRule, today: NaiveDate) {
        let mut state = self.state.write();
        let bucket = match state.buckets.get_mut(bucket_name) {
            Some(b) => b,
            None => return,
        };

        // Collect keys to expire
        let mut keys_to_delete: Vec<String> = Vec::new();
        // Collect keys to transition (key, new_storage_class)
        let mut keys_to_transition: Vec<(String, String)> = Vec::new();

        for (key, obj) in bucket.objects.iter() {
            // Check prefix filter
            if let Some(ref prefix) = rule.prefix {
                if !prefix.is_empty() && !key.starts_with(prefix) {
                    continue;
                }
            }

            // Check tag filter
            if let Some(ref tag_filter) = rule.tag_filter {
                let matches = obj
                    .tags
                    .get(&tag_filter.key)
                    .map(|v| v == &tag_filter.value)
                    .unwrap_or(false);
                if !matches {
                    continue;
                }
            }

            // Check expiration by Days
            if let Some(days) = rule.expiration_days {
                let age = today
                    .signed_duration_since(obj.last_modified.date_naive())
                    .num_days();
                if age >= days as i64 {
                    keys_to_delete.push(key.clone());
                    continue;
                }
            }

            // Check expiration by Date
            if let Some(ref date) = rule.expiration_date {
                if &today >= date {
                    keys_to_delete.push(key.clone());
                    continue;
                }
            }

            // Check transition by Days
            for transition in &rule.transitions {
                let should_transition = if let Some(days) = transition.days {
                    let age = today
                        .signed_duration_since(obj.last_modified.date_naive())
                        .num_days();
                    age >= days as i64
                } else if let Some(ref date) = transition.date {
                    &today >= date
                } else {
                    false
                };

                if should_transition && obj.storage_class != transition.storage_class {
                    keys_to_transition.push((key.clone(), transition.storage_class.clone()));
                    break; // Only apply first matching transition
                }
            }
        }

        // Apply deletions
        if !keys_to_delete.is_empty() {
            tracing::info!(
                bucket = %bucket_name,
                count = keys_to_delete.len(),
                "S3 lifecycle: expiring objects"
            );
            for key in &keys_to_delete {
                bucket.objects.remove(key);
            }
        }

        // Apply transitions
        if !keys_to_transition.is_empty() {
            tracing::info!(
                bucket = %bucket_name,
                count = keys_to_transition.len(),
                "S3 lifecycle: transitioning object storage classes"
            );
            for (key, new_class) in &keys_to_transition {
                if let Some(obj) = bucket.objects.get_mut(key) {
                    obj.storage_class = new_class.clone();
                }
            }
        }
    }
}

/// A parsed lifecycle rule.
struct LifecycleRule {
    status: String,
    prefix: Option<String>,
    tag_filter: Option<TagFilter>,
    expiration_days: Option<u32>,
    expiration_date: Option<NaiveDate>,
    transitions: Vec<Transition>,
}

struct TagFilter {
    key: String,
    value: String,
}

struct Transition {
    days: Option<u32>,
    date: Option<NaiveDate>,
    storage_class: String,
}

/// Parse lifecycle configuration XML into rules.
fn parse_lifecycle_rules(xml: &str) -> Option<Vec<LifecycleRule>> {
    let mut rules = Vec::new();
    let mut remaining = xml;

    while let Some(rule_start) = remaining.find("<Rule>") {
        let after = &remaining[rule_start + 6..];
        let rule_end = after.find("</Rule>")?;
        let rule_body = &after[..rule_end];

        let status = extract_tag(rule_body, "Status").unwrap_or_default();

        // Parse prefix — can be at rule level or inside <Filter>
        let prefix = if let Some(filter_body) = extract_block(rule_body, "Filter") {
            // Check for <Prefix> inside Filter
            let filter_prefix = extract_tag(filter_body, "Prefix");
            // Also check for <And><Prefix> pattern
            if filter_prefix.is_some() {
                filter_prefix
            } else if let Some(and_body) = extract_block(filter_body, "And") {
                extract_tag(and_body, "Prefix")
            } else {
                None
            }
        } else {
            extract_tag(rule_body, "Prefix")
        };

        // Parse tag filter from <Filter><Tag> or <Filter><And><Tag>
        let tag_filter = if let Some(filter_body) = extract_block(rule_body, "Filter") {
            parse_tag_filter(filter_body)
        } else {
            None
        };

        // Parse expiration
        let (expiration_days, expiration_date) =
            if let Some(exp_body) = extract_block(rule_body, "Expiration") {
                let days = extract_tag(exp_body, "Days").and_then(|s| s.parse::<u32>().ok());
                let date = extract_tag(exp_body, "Date").and_then(|s| parse_date(&s));
                (days, date)
            } else {
                (None, None)
            };

        // Parse transitions
        let mut transitions = Vec::new();
        let mut trans_remaining = rule_body;
        while let Some(t_start) = trans_remaining.find("<Transition>") {
            let t_after = &trans_remaining[t_start + 12..];
            if let Some(t_end) = t_after.find("</Transition>") {
                let t_body = &t_after[..t_end];
                let days = extract_tag(t_body, "Days").and_then(|s| s.parse::<u32>().ok());
                let date = extract_tag(t_body, "Date").and_then(|s| parse_date(&s));
                let storage_class =
                    extract_tag(t_body, "StorageClass").unwrap_or_else(|| "GLACIER".to_string());
                transitions.push(Transition {
                    days,
                    date,
                    storage_class,
                });
                trans_remaining = &t_after[t_end + 13..];
            } else {
                break;
            }
        }

        rules.push(LifecycleRule {
            status,
            prefix,
            tag_filter,
            expiration_days,
            expiration_date,
            transitions,
        });

        remaining = &after[rule_end + 7..];
    }

    Some(rules)
}

/// Extract text content of a simple XML tag, e.g. `<Days>30</Days>` -> "30".
fn extract_tag(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)?;
    let content_start = start + open.len();
    let end = body[content_start..].find(&close)?;
    Some(body[content_start..content_start + end].trim().to_string())
}

/// Extract the body of a block element, e.g. `<Filter>...</Filter>` -> "...".
fn extract_block<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)?;
    let content_start = start + open.len();
    let end = body[content_start..].find(&close)?;
    Some(&body[content_start..content_start + end])
}

fn parse_tag_filter(filter_body: &str) -> Option<TagFilter> {
    // Try direct <Tag> inside <Filter>
    if let Some(tag_body) = extract_block(filter_body, "Tag") {
        let key = extract_tag(tag_body, "Key")?;
        let value = extract_tag(tag_body, "Value").unwrap_or_default();
        return Some(TagFilter { key, value });
    }
    // Try <And><Tag> inside <Filter>
    if let Some(and_body) = extract_block(filter_body, "And") {
        if let Some(tag_body) = extract_block(and_body, "Tag") {
            let key = extract_tag(tag_body, "Key")?;
            let value = extract_tag(tag_body, "Value").unwrap_or_default();
            return Some(TagFilter { key, value });
        }
    }
    None
}

/// Parse a date string like "2024-01-01" or "2024-01-01T00:00:00.000Z".
fn parse_date(s: &str) -> Option<NaiveDate> {
    // Try YYYY-MM-DD first
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d);
    }
    // Try ISO 8601 with time
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.date_naive());
    }
    // Try with T and Z suffix
    if let Some(date_part) = s.split('T').next() {
        if let Ok(d) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
            return Some(d);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_expiration_days_rule() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Filter><Prefix>logs/</Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>30</Days></Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let rules = parse_lifecycle_rules(xml).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].status, "Enabled");
        assert_eq!(rules[0].prefix.as_deref(), Some("logs/"));
        assert_eq!(rules[0].expiration_days, Some(30));
    }

    #[test]
    fn parse_expiration_date_rule() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Filter><Prefix></Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><Date>2024-06-01</Date></Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let rules = parse_lifecycle_rules(xml).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].expiration_date,
            Some(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap())
        );
    }

    #[test]
    fn parse_transition_rule() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Filter><Prefix>archive/</Prefix></Filter>
                <Status>Enabled</Status>
                <Transition>
                    <Days>90</Days>
                    <StorageClass>GLACIER</StorageClass>
                </Transition>
                <Transition>
                    <Days>365</Days>
                    <StorageClass>DEEP_ARCHIVE</StorageClass>
                </Transition>
            </Rule>
        </LifecycleConfiguration>"#;

        let rules = parse_lifecycle_rules(xml).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].transitions.len(), 2);
        assert_eq!(rules[0].transitions[0].days, Some(90));
        assert_eq!(rules[0].transitions[0].storage_class, "GLACIER");
        assert_eq!(rules[0].transitions[1].days, Some(365));
        assert_eq!(rules[0].transitions[1].storage_class, "DEEP_ARCHIVE");
    }

    #[test]
    fn parse_disabled_rule() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Filter><Prefix></Prefix></Filter>
                <Status>Disabled</Status>
                <Expiration><Days>1</Days></Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let rules = parse_lifecycle_rules(xml).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].status, "Disabled");
    }

    #[test]
    fn parse_tag_filter_rule() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Filter>
                    <Tag><Key>env</Key><Value>test</Value></Tag>
                </Filter>
                <Status>Enabled</Status>
                <Expiration><Days>7</Days></Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let rules = parse_lifecycle_rules(xml).unwrap();
        assert_eq!(rules.len(), 1);
        let tag = rules[0].tag_filter.as_ref().unwrap();
        assert_eq!(tag.key, "env");
        assert_eq!(tag.value, "test");
    }

    #[test]
    fn parse_multiple_rules() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Filter><Prefix>a/</Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>10</Days></Expiration>
            </Rule>
            <Rule>
                <Filter><Prefix>b/</Prefix></Filter>
                <Status>Enabled</Status>
                <Expiration><Days>20</Days></Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let rules = parse_lifecycle_rules(xml).unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].prefix.as_deref(), Some("a/"));
        assert_eq!(rules[0].expiration_days, Some(10));
        assert_eq!(rules[1].prefix.as_deref(), Some("b/"));
        assert_eq!(rules[1].expiration_days, Some(20));
    }
}
