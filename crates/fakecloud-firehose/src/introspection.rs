//! Firehose admin introspection helpers consumed by
//! `/_fakecloud/firehose/*` routes.
//!
//! These read the in-memory state cross-account/region and produce
//! assertion-friendly rows. They intentionally bypass IAM — admin
//! endpoints never authenticate.

use chrono::{DateTime, Utc};

use crate::state::{DeliveryStream, SharedFirehoseState};

/// Server-side encryption summary derived from a stream's persisted
/// encryption config. `None`/unset config reads as DISABLED.
#[derive(Debug, Clone)]
pub struct EncryptionRow {
    /// ENABLED / DISABLED.
    pub status: String,
    pub key_type: Option<String>,
    pub key_arn: Option<String>,
}

/// One delivery stream flattened for introspection.
#[derive(Debug, Clone)]
pub struct DeliveryStreamRow {
    pub account_id: String,
    pub name: String,
    pub arn: String,
    /// DirectPut / KinesisStreamAsSource.
    pub stream_type: String,
    /// CREATING / ACTIVE / ...
    pub status: String,
    pub encryption: EncryptionRow,
    pub destination_count: usize,
    pub create_timestamp: DateTime<Utc>,
    pub last_update_timestamp: Option<DateTime<Utc>>,
}

fn encryption_row(stream: &DeliveryStream) -> EncryptionRow {
    match &stream.encryption {
        Some(cfg) => EncryptionRow {
            status: cfg.status.clone(),
            key_type: cfg.key_type.clone(),
            key_arn: cfg.key_arn.clone(),
        },
        None => EncryptionRow {
            status: "DISABLED".to_string(),
            key_type: None,
            key_arn: None,
        },
    }
}

fn stream_to_row(account_id: &str, stream: &DeliveryStream) -> DeliveryStreamRow {
    DeliveryStreamRow {
        account_id: account_id.to_string(),
        name: stream.name.clone(),
        arn: stream.arn.clone(),
        stream_type: stream.stream_type.clone(),
        status: stream.status.clone(),
        encryption: encryption_row(stream),
        destination_count: stream.destination.iter().count(),
        create_timestamp: stream.created_at,
        last_update_timestamp: Some(stream.last_update),
    }
}

/// Flatten every delivery stream across all accounts and regions into one
/// list sorted by (account, name).
pub fn list_all_delivery_streams(state: &SharedFirehoseState) -> Vec<DeliveryStreamRow> {
    let accounts = state.read();
    let mut rows: Vec<DeliveryStreamRow> = Vec::new();
    for (account_id, acct) in &accounts.accounts {
        for streams in acct.streams_by_region.values() {
            for stream in streams.values() {
                rows.push(stream_to_row(account_id, stream));
            }
        }
    }
    rows.sort_by(|a, b| {
        a.account_id
            .cmp(&b.account_id)
            .then_with(|| a.name.cmp(&b.name))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EncryptionConfig, FirehoseAccounts, S3Destination};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn stream(account: &str, name: &str, encryption: Option<EncryptionConfig>) -> DeliveryStream {
        DeliveryStream {
            name: name.to_string(),
            arn: format!("arn:aws:firehose:us-east-1:{account}:deliverystream/{name}"),
            status: "ACTIVE".to_string(),
            stream_type: "DirectPut".to_string(),
            created_at: Utc::now(),
            last_update: Utc::now(),
            version_id: "1".to_string(),
            destination: Some(S3Destination {
                destination_id: "destinationId-1".to_string(),
                role_arn: format!("arn:aws:iam::{account}:role/firehose"),
                bucket_arn: "arn:aws:s3:::bucket".to_string(),
                prefix: None,
                error_output_prefix: None,
                buffering_size_mb: None,
                buffering_interval_seconds: None,
                compression_format: None,
                processing_configuration: None,
                data_format_conversion_configuration: None,
                cloudwatch_logging_options: None,
                custom_time_zone: None,
                s3_backup_mode: None,
                file_extension: None,
            }),
            tags: BTreeMap::new(),
            encryption,
            extra_destinations: BTreeMap::new(),
        }
    }

    fn state_with(streams: Vec<(&str, DeliveryStream)>) -> SharedFirehoseState {
        let mut accounts = FirehoseAccounts::new();
        for (account, s) in streams {
            accounts
                .get_or_create(account, "us-east-1")
                .streams_mut("us-east-1")
                .insert(s.name.clone(), s);
        }
        Arc::new(parking_lot::RwLock::new(accounts))
    }

    #[test]
    fn lists_delivery_streams_across_accounts() {
        let state = state_with(vec![
            ("222222222222", stream("222222222222", "other", None)),
            ("111111111111", stream("111111111111", "logs", None)),
        ]);

        let streams = list_all_delivery_streams(&state);
        assert_eq!(streams.len(), 2);
        assert_eq!(streams[0].account_id, "111111111111");
        assert_eq!(streams[0].name, "logs");
        assert_eq!(streams[0].stream_type, "DirectPut");
        assert_eq!(streams[0].destination_count, 1);
        assert_eq!(streams[0].encryption.status, "DISABLED");
        assert!(streams[0].last_update_timestamp.is_some());
        assert_eq!(streams[1].account_id, "222222222222");
        assert_eq!(streams[1].name, "other");
    }

    #[test]
    fn reflects_enabled_encryption() {
        let state = state_with(vec![(
            "111111111111",
            stream(
                "111111111111",
                "enc",
                Some(EncryptionConfig {
                    status: "ENABLED".to_string(),
                    key_type: Some("AWS_OWNED_CMK".to_string()),
                    key_arn: None,
                }),
            ),
        )]);

        let streams = list_all_delivery_streams(&state);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].encryption.status, "ENABLED");
        assert_eq!(
            streams[0].encryption.key_type.as_deref(),
            Some("AWS_OWNED_CMK")
        );
    }
}
