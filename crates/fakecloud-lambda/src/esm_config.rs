//! The event source mapping settings that belong to one kind of source:
//! `AmazonManagedKafkaEventSourceConfig` (Amazon MSK),
//! `SelfManagedKafkaEventSourceConfig` (self-managed Apache Kafka),
//! `ScalingConfig` (Amazon SQS), `ProvisionedPollerConfig` (Amazon SQS and
//! both Kafka kinds) and `LoggingConfig` (both Kafka kinds).
//!
//! Three layers, shared by CreateEventSourceMapping, UpdateEventSourceMapping
//! and CloudFormation's AWS::Lambda::EventSourceMapping:
//! - [`validate_shapes`]: each member against its Smithy `@length`,
//!   `@pattern`, `@range` and enum (run on every request);
//! - which settings the mapping's source takes, and the per-source poller
//!   ranges the model documents;
//! - what a mapping stores: on create, a Kafka mapping always carries its
//!   Kafka config, its `ConsumerGroupId` defaulting to the mapping's UUID,
//!   and provisioned mode fills in the documented `MinimumPollers` /
//!   `MaximumPollers` defaults; on update, `ConsumerGroupId` (and a
//!   self-managed mapping's `ConsumptionMode`) cannot change, as the model
//!   documents, while `SchemaRegistryConfig` and the other settings are
//!   replaced, and an empty `ScalingConfig` / `ProvisionedPollerConfig` /
//!   `LoggingConfig` removes the setting.

use http::StatusCode;
use serde_json::{Map, Value};

use fakecloud_core::service::AwsServiceError;

use crate::state::EventSourceMapping;

const AMAZON_MANAGED_KAFKA: &str = "AmazonManagedKafkaEventSourceConfig";
const SELF_MANAGED_KAFKA: &str = "SelfManagedKafkaEventSourceConfig";
const SCALING: &str = "ScalingConfig";
const PROVISIONED_POLLER: &str = "ProvisionedPollerConfig";
const LOGGING: &str = "LoggingConfig";

/// The kind of source a mapping reads, as far as these settings care.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappingSource {
    Sqs,
    Msk,
    SelfManagedKafka,
    /// Kinesis, DynamoDB Streams, Amazon MQ, DocumentDB.
    Other,
}

impl MappingSource {
    /// The source of a mapping reading `event_source_arn`, or a self-managed
    /// Kafka cluster when it has a `SelfManagedEventSource` instead.
    pub fn of(event_source_arn: &str, self_managed_event_source: bool) -> Self {
        if event_source_arn.is_empty() && self_managed_event_source {
            return Self::SelfManagedKafka;
        }
        match event_source_arn.split(':').nth(2) {
            Some("sqs") => Self::Sqs,
            Some("kafka") => Self::Msk,
            _ => Self::Other,
        }
    }

    fn of_mapping(esm: &EventSourceMapping) -> Self {
        Self::of(
            &esm.event_source_arn,
            esm.self_managed_event_source.is_some(),
        )
    }

    fn is_kafka(self) -> bool {
        matches!(self, Self::Msk | Self::SelfManagedKafka)
    }

    /// `ProvisionedPollerConfig`'s documented defaults and bounds for this
    /// source: `(default minimum, lowest minimum, default maximum, lowest
    /// maximum, highest maximum)`.
    fn poller_bounds(self) -> (i64, i64, i64, i64, i64) {
        match self {
            Self::Sqs => (2, 2, 200, 2, 10_000),
            _ => (1, 1, 200, 1, 2_000),
        }
    }
}

/// `BatchSize` when a mapping is created without one: 10 for Amazon SQS, 100
/// for every other source (the `BatchSize` member's documented defaults).
pub fn default_batch_size(event_source_arn: &str, self_managed_event_source: bool) -> i64 {
    match MappingSource::of(event_source_arn, self_managed_event_source) {
        MappingSource::Sqs => 10,
        _ => 100,
    }
}

fn invalid_parameter(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        message.into(),
    )
}

/// Lambda's input-validation refusal for a nested member, the same
/// `ValidationException` "1 validation error detected" it answers a
/// top-level constraint violation with (`EventSourceArn`, recorded).
/// `value` is shown as given (quoted for strings by the callers).
fn violation(value: &str, member: &str, constraint: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!(
            "1 validation error detected: Value {value} at '{member}' failed to satisfy constraint: {constraint}"
        ),
    )
}

fn check_string_len(
    value: &str,
    member: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    let len = value.chars().count();
    if len < min {
        return Err(violation(
            &format!("'{value}'"),
            member,
            &format!("Member must have length greater than or equal to {min}"),
        ));
    }
    if len > max {
        return Err(violation(
            &format!("'{value}'"),
            member,
            &format!("Member must have length less than or equal to {max}"),
        ));
    }
    Ok(())
}

/// `value` satisfies `^[<extra>a-zA-Z0-9]*$`.
fn check_chars(
    value: &str,
    member: &str,
    extra: &str,
    pattern: &str,
) -> Result<(), AwsServiceError> {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || extra.contains(c))
    {
        Ok(())
    } else {
        Err(violation(
            &format!("'{value}'"),
            member,
            &format!("Member must satisfy regular expression pattern: {pattern}"),
        ))
    }
}

fn check_enum(value: &Value, member: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(s) if allowed.contains(&s.as_str()) => Ok(()),
        other => {
            let shown = match other {
                Value::String(s) => format!("'{s}'"),
                v => v.to_string(),
            };
            Err(violation(
                &shown,
                member,
                &format!(
                    "Member must satisfy enum value set: [{}]",
                    allowed.join(", ")
                ),
            ))
        }
    }
}

fn check_int(value: &Value, member: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    let Some(n) = value.as_i64() else {
        return match value {
            Value::Null => Ok(()),
            other => Err(violation(
                &other.to_string(),
                member,
                "Member must be an integer",
            )),
        };
    };
    if n < min {
        return Err(violation(
            &n.to_string(),
            member,
            &format!("Member must have value greater than or equal to {min}"),
        ));
    }
    if n > max {
        return Err(violation(
            &n.to_string(),
            member,
            &format!("Member must have value less than or equal to {max}"),
        ));
    }
    Ok(())
}

fn check_string(
    value: &Value,
    member: &str,
    check: impl FnOnce(&str) -> Result<(), AwsServiceError>,
) -> Result<(), AwsServiceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(s) => check(s),
        other => Err(violation(
            &other.to_string(),
            member,
            "Member must be a string",
        )),
    }
}

/// A structure member: absent / null is `None`, a non-object is refused.
fn object<'a>(
    parent: &'a Value,
    key: &str,
    member: &str,
) -> Result<Option<&'a Map<String, Value>>, AwsServiceError> {
    structure(parent.get(key), member)
}

fn structure<'a>(
    value: Option<&'a Value>,
    member: &str,
) -> Result<Option<&'a Map<String, Value>>, AwsServiceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(map)) => Ok(Some(map)),
        Some(other) => Err(violation(
            &other.to_string(),
            member,
            "Member must be a structure",
        )),
    }
}

fn list<'a>(
    parent: &'a Map<String, Value>,
    key: &str,
    member: &str,
) -> Result<&'a [Value], AwsServiceError> {
    match parent.get(key) {
        None | Some(Value::Null) => Ok(&[]),
        Some(Value::Array(items)) => Ok(items),
        Some(other) => Err(violation(
            &other.to_string(),
            member,
            "Member must be a list",
        )),
    }
}

fn get<'a>(map: &'a Map<String, Value>, key: &str) -> &'a Value {
    map.get(key).unwrap_or(&Value::Null)
}

const CONSUMER_GROUP_ID_PATTERN: &str = "^[ a-zA-Z0-9-\\/*:_+=.@-]*$";
const SCHEMA_REGISTRY_URI_PATTERN: &str = "^[a-zA-Z0-9-\\/*:_+=.@-]*$";
const POLLER_GROUP_NAME_PATTERN: &str = "^[a-zA-Z0-9-_]*$";
const ARN_PATTERN: &str = "^arn:(aws[a-zA-Z0-9-]*):([a-zA-Z0-9\\-])+:((eusc-)?[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\\d{1})?:(\\d{12})?:(.*)$";

/// `KafkaSchemaRegistryConfig` under `prefix`.
fn validate_schema_registry(cfg: &Map<String, Value>, prefix: &str) -> Result<(), AwsServiceError> {
    let uri_member = format!("{prefix}.schemaRegistryURI");
    check_string(get(cfg, "SchemaRegistryURI"), &uri_member, |s| {
        check_string_len(s, &uri_member, 1, 10_000)?;
        check_chars(s, &uri_member, "-/*:_+=.@", SCHEMA_REGISTRY_URI_PATTERN)
    })?;
    check_enum(
        get(cfg, "EventRecordFormat"),
        &format!("{prefix}.eventRecordFormat"),
        &["JSON", "SOURCE"],
    )?;
    let access_member = format!("{prefix}.accessConfigs");
    for (i, item) in list(cfg, "AccessConfigs", &access_member)?
        .iter()
        .enumerate()
    {
        let item_member = format!("{access_member}.{}", i + 1);
        let Some(access) = item.as_object() else {
            return Err(violation(
                &item.to_string(),
                &item_member,
                "Member must be a structure",
            ));
        };
        check_enum(
            get(access, "Type"),
            &format!("{item_member}.type"),
            &[
                "BASIC_AUTH",
                "CLIENT_CERTIFICATE_TLS_AUTH",
                "SERVER_ROOT_CA_CERTIFICATE",
                "OAUTHBEARER_AUTH",
            ],
        )?;
        let uri_member = format!("{item_member}.uRI");
        check_string(get(access, "URI"), &uri_member, |s| {
            check_string_len(s, &uri_member, 0, 10_000)?;
            if crate::event_source::satisfies_arn_pattern(s) {
                Ok(())
            } else {
                Err(violation(
                    &format!("'{s}'"),
                    &uri_member,
                    &format!("Member must satisfy regular expression pattern: {ARN_PATTERN}"),
                ))
            }
        })?;
    }
    let validation_member = format!("{prefix}.schemaValidationConfigs");
    for (i, item) in list(cfg, "SchemaValidationConfigs", &validation_member)?
        .iter()
        .enumerate()
    {
        let item_member = format!("{validation_member}.{}", i + 1);
        let Some(validation) = item.as_object() else {
            return Err(violation(
                &item.to_string(),
                &item_member,
                "Member must be a structure",
            ));
        };
        check_enum(
            get(validation, "Attribute"),
            &format!("{item_member}.attribute"),
            &["KEY", "VALUE"],
        )?;
    }
    Ok(())
}

/// `AmazonManagedKafkaEventSourceConfig` / `SelfManagedKafkaEventSourceConfig`.
fn validate_kafka(body: &Value, key: &str, self_managed: bool) -> Result<(), AwsServiceError> {
    let prefix = lower_camel(key);
    let Some(cfg) = object(body, key, &prefix)? else {
        return Ok(());
    };
    let group_member = format!("{prefix}.consumerGroupId");
    check_string(get(cfg, "ConsumerGroupId"), &group_member, |s| {
        check_string_len(s, &group_member, 1, 200)?;
        check_chars(s, &group_member, " -/*:_+=.@", CONSUMER_GROUP_ID_PATTERN)
    })?;
    let registry_member = format!("{prefix}.schemaRegistryConfig");
    if let Some(registry) = structure(cfg.get("SchemaRegistryConfig"), &registry_member)? {
        validate_schema_registry(registry, &registry_member)?;
    }
    if self_managed {
        check_enum(
            get(cfg, "ConsumptionMode"),
            &format!("{prefix}.consumptionMode"),
            &["Stream", "Queue"],
        )?;
    }
    Ok(())
}

fn lower_camel(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// Every member of the per-source settings against its Smithy constraints.
/// CreateEventSourceMapping and UpdateEventSourceMapping both run it.
pub(crate) fn validate_shapes(body: &Value) -> Result<(), AwsServiceError> {
    validate_kafka(body, AMAZON_MANAGED_KAFKA, false)?;
    validate_kafka(body, SELF_MANAGED_KAFKA, true)?;
    if let Some(scaling) = object(body, SCALING, "scalingConfig")? {
        check_int(
            get(scaling, "MaximumConcurrency"),
            "scalingConfig.maximumConcurrency",
            2,
            1000,
        )?;
    }
    if let Some(poller) = object(body, PROVISIONED_POLLER, "provisionedPollerConfig")? {
        check_int(
            get(poller, "MinimumPollers"),
            "provisionedPollerConfig.minimumPollers",
            1,
            200,
        )?;
        check_int(
            get(poller, "MaximumPollers"),
            "provisionedPollerConfig.maximumPollers",
            1,
            10_000,
        )?;
        let member = "provisionedPollerConfig.pollerGroupName";
        check_string(get(poller, "PollerGroupName"), member, |s| {
            check_string_len(s, member, 0, 128)?;
            check_chars(s, member, "-_", POLLER_GROUP_NAME_PATTERN)
        })?;
    }
    if let Some(logging) = object(body, LOGGING, "loggingConfig")? {
        check_enum(
            get(logging, "SystemLogLevel"),
            "loggingConfig.systemLogLevel",
            &["DEBUG", "INFO", "WARN"],
        )?;
    }
    Ok(())
}

/// A structure the request carries with at least one member set. An empty
/// one (`{}`) is how a caller removes a setting on update.
fn non_empty<'a>(body: &'a Value, key: &str) -> Option<&'a Map<String, Value>> {
    body.get(key)
        .and_then(Value::as_object)
        .filter(|m| m.values().any(|v| !v.is_null()))
}

/// Refuse a setting the mapping's source does not take.
fn check_applies(body: &Value, source: MappingSource) -> Result<(), AwsServiceError> {
    if non_empty(body, AMAZON_MANAGED_KAFKA).is_some() && source != MappingSource::Msk {
        return Err(invalid_parameter(
            "AmazonManagedKafkaEventSourceConfig is only supported for Amazon MSK event sources.",
        ));
    }
    if non_empty(body, SELF_MANAGED_KAFKA).is_some() && source != MappingSource::SelfManagedKafka {
        return Err(invalid_parameter(
            "SelfManagedKafkaEventSourceConfig is only supported for self-managed Apache Kafka event sources.",
        ));
    }
    if non_empty(body, SCALING).is_some() && source != MappingSource::Sqs {
        return Err(invalid_parameter(
            "ScalingConfig is only supported for Amazon SQS event sources.",
        ));
    }
    if non_empty(body, PROVISIONED_POLLER).is_some()
        && !(source == MappingSource::Sqs || source.is_kafka())
    {
        return Err(invalid_parameter(
            "ProvisionedPollerConfig is only supported for Amazon SQS, Amazon MSK and self-managed Apache Kafka event sources.",
        ));
    }
    if non_empty(body, LOGGING).is_some() && !source.is_kafka() {
        return Err(invalid_parameter(
            "LoggingConfig is only supported for Amazon MSK and self-managed Apache Kafka event sources.",
        ));
    }
    Ok(())
}

/// The stored `ProvisionedPollerConfig` for a request's one: the documented
/// defaults filled in, the per-source bounds checked. `None` for an empty
/// config, which leaves (or puts) the mapping in on-demand mode.
fn provisioned_poller(
    body: &Value,
    source: MappingSource,
) -> Result<Option<Value>, AwsServiceError> {
    let Some(cfg) = non_empty(body, PROVISIONED_POLLER) else {
        return Ok(None);
    };
    let (default_min, lowest_min, default_max, lowest_max, highest_max) = source.poller_bounds();
    let min = cfg
        .get("MinimumPollers")
        .and_then(Value::as_i64)
        .unwrap_or(default_min);
    let max = cfg
        .get("MaximumPollers")
        .and_then(Value::as_i64)
        .unwrap_or(default_max);
    let kind = if source == MappingSource::Sqs {
        "Amazon SQS"
    } else {
        "Kafka"
    };
    if min < lowest_min {
        return Err(invalid_parameter(format!(
            "MinimumPollers must be at least {lowest_min} for {kind} event sources."
        )));
    }
    if !(lowest_max..=highest_max).contains(&max) {
        return Err(invalid_parameter(format!(
            "MaximumPollers must be between {lowest_max} and {highest_max} for {kind} event sources."
        )));
    }
    if min > max {
        return Err(invalid_parameter(
            "MinimumPollers must be less than or equal to MaximumPollers.",
        ));
    }
    let mut stored = Map::new();
    stored.insert("MinimumPollers".into(), min.into());
    stored.insert("MaximumPollers".into(), max.into());
    if let Some(group) = cfg.get("PollerGroupName").and_then(Value::as_str) {
        if source == MappingSource::Sqs {
            return Err(invalid_parameter(
                "PollerGroupName is only supported for Amazon MSK and self-managed Apache Kafka event sources.",
            ));
        }
        stored.insert("PollerGroupName".into(), group.into());
    }
    Ok(Some(Value::Object(stored)))
}

/// A request's structure with its null members dropped, `None` when empty.
fn stored_object(body: &Value, key: &str) -> Option<Value> {
    non_empty(body, key).map(|m| {
        Value::Object(
            m.iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    })
}

/// A new Kafka mapping's config: the request's, `ConsumerGroupId`
/// defaulting to the mapping's UUID.
fn kafka_on_create(body: &Value, key: &str, uuid: &str) -> Value {
    let mut cfg = match stored_object(body, key) {
        Some(Value::Object(m)) => m,
        _ => Map::new(),
    };
    cfg.entry("ConsumerGroupId")
        .or_insert_with(|| Value::String(uuid.to_string()));
    Value::Object(cfg)
}

/// A mapping's `Tags`: the API's `{key: value}` map, or CloudFormation's
/// `[{Key, Value}]` list. Non-string values are skipped, as for function
/// tags.
pub fn tags(body: &Value) -> std::collections::BTreeMap<String, String> {
    match body.get("Tags") {
        Some(Value::Object(m)) => m
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|t| {
                Some((
                    t.get("Key")?.as_str()?.to_string(),
                    t.get("Value")?.as_str()?.to_string(),
                ))
            })
            .collect(),
        _ => Default::default(),
    }
}

/// The per-source settings a new mapping stores.
#[derive(Debug, Default, Clone)]
pub struct SourceConfigs {
    pub amazon_managed_kafka: Option<Value>,
    pub self_managed_kafka: Option<Value>,
    pub scaling: Option<Value>,
    pub provisioned_poller: Option<Value>,
    pub logging: Option<Value>,
}

/// The settings of a mapping CreateEventSourceMapping (or CloudFormation)
/// creates with UUID `uuid` from `body`, refusing those its source does not
/// take. `body` has passed [`validate_shapes`].
pub fn create_configs(body: &Value, uuid: &str) -> Result<SourceConfigs, AwsServiceError> {
    let source = MappingSource::of(
        body.get("EventSourceArn")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        body.get("SelfManagedEventSource")
            .is_some_and(Value::is_object),
    );
    check_applies(body, source)?;
    Ok(SourceConfigs {
        amazon_managed_kafka: (source == MappingSource::Msk)
            .then(|| kafka_on_create(body, AMAZON_MANAGED_KAFKA, uuid)),
        self_managed_kafka: (source == MappingSource::SelfManagedKafka)
            .then(|| kafka_on_create(body, SELF_MANAGED_KAFKA, uuid)),
        scaling: stored_object(body, SCALING),
        provisioned_poller: provisioned_poller(body, source)?,
        logging: stored_object(body, LOGGING),
    })
}

/// A stored Kafka config updated by a request's: `ConsumerGroupId` and
/// `ConsumptionMode` are fixed at creation (resending the same value is
/// fine), `SchemaRegistryConfig` is replaced (an empty one removes it).
fn kafka_on_update(
    stored: Option<&Value>,
    body: &Value,
    key: &str,
    applies: bool,
    uuid: &str,
) -> Result<Option<Value>, AwsServiceError> {
    let request = match body.get(key).and_then(Value::as_object) {
        Some(request) if applies => request,
        _ => return Ok(stored.cloned()),
    };
    let mut cfg = stored
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let requested_group = request.get("ConsumerGroupId").and_then(Value::as_str);
    // A mapping stored before its Kafka config was recorded has no group on
    // file: the one the caller resends is its group.
    let current_group = cfg
        .get("ConsumerGroupId")
        .and_then(Value::as_str)
        .or(requested_group)
        .unwrap_or(uuid)
        .to_string();
    if let Some(group) = requested_group {
        if group != current_group {
            return Err(invalid_parameter(format!(
                "ConsumerGroupId cannot be updated. The event source mapping's consumer group ID is {current_group}."
            )));
        }
    }
    if let Some(mode) = request.get("ConsumptionMode").and_then(Value::as_str) {
        let current = cfg
            .get("ConsumptionMode")
            .and_then(Value::as_str)
            .unwrap_or("Stream");
        if mode != current {
            return Err(invalid_parameter(format!(
                "ConsumptionMode cannot be updated. The event source mapping's consumption mode is {current}."
            )));
        }
    }
    match request.get("SchemaRegistryConfig") {
        None => {}
        Some(Value::Object(registry)) if registry.values().any(|v| !v.is_null()) => {
            cfg.insert(
                "SchemaRegistryConfig".into(),
                Value::Object(
                    registry
                        .iter()
                        .filter(|(_, v)| !v.is_null())
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                ),
            );
        }
        Some(_) => {
            cfg.remove("SchemaRegistryConfig");
        }
    }
    cfg.entry("ConsumerGroupId")
        .or_insert_with(|| Value::String(current_group));
    Ok(Some(Value::Object(cfg)))
}

/// A replaceable setting on update: absent keeps it, an empty structure
/// removes it, anything else replaces it.
fn replaced(stored: &Option<Value>, body: &Value, key: &str) -> Option<Value> {
    if body.get(key).is_some_and(Value::is_object) {
        stored_object(body, key)
    } else {
        stored.clone()
    }
}

/// Apply an UpdateEventSourceMapping request's per-source settings to
/// `esm`. Every check runs before anything changes, so a refused request
/// leaves the mapping as it was. `body` has passed [`validate_shapes`].
pub fn apply_update(esm: &mut EventSourceMapping, body: &Value) -> Result<(), AwsServiceError> {
    let source = MappingSource::of_mapping(esm);
    check_applies(body, source)?;
    let amazon_managed_kafka = kafka_on_update(
        esm.amazon_managed_kafka_event_source_config.as_ref(),
        body,
        AMAZON_MANAGED_KAFKA,
        source == MappingSource::Msk,
        &esm.uuid,
    )?;
    let self_managed_kafka = kafka_on_update(
        esm.self_managed_kafka_event_source_config.as_ref(),
        body,
        SELF_MANAGED_KAFKA,
        source == MappingSource::SelfManagedKafka,
        &esm.uuid,
    )?;
    let provisioned_poller = if body.get(PROVISIONED_POLLER).is_some_and(Value::is_object) {
        provisioned_poller(body, source)?
    } else {
        esm.provisioned_poller_config.clone()
    };
    esm.scaling_config = replaced(&esm.scaling_config, body, SCALING);
    esm.logging_config = replaced(&esm.logging_config, body, LOGGING);
    esm.amazon_managed_kafka_event_source_config = amazon_managed_kafka;
    esm.self_managed_kafka_event_source_config = self_managed_kafka;
    esm.provisioned_poller_config = provisioned_poller;
    Ok(())
}

/// The settings a mapping's response carries, in the
/// `EventSourceMappingConfiguration` member names. A Kafka mapping always
/// carries its Kafka config; one stored before it was recorded reports the
/// default consumer group, its UUID.
pub(crate) fn render(esm: &EventSourceMapping, out: &mut Map<String, Value>) {
    let default_kafka = || Some(serde_json::json!({"ConsumerGroupId": esm.uuid}));
    let source = MappingSource::of_mapping(esm);
    let amazon_managed_kafka = esm
        .amazon_managed_kafka_event_source_config
        .clone()
        .or_else(|| (source == MappingSource::Msk).then(default_kafka).flatten());
    let self_managed_kafka = esm
        .self_managed_kafka_event_source_config
        .clone()
        .or_else(|| {
            (source == MappingSource::SelfManagedKafka)
                .then(default_kafka)
                .flatten()
        });
    for (key, value) in [
        (AMAZON_MANAGED_KAFKA, &amazon_managed_kafka),
        (SELF_MANAGED_KAFKA, &self_managed_kafka),
        (SCALING, &esm.scaling_config),
        (PROVISIONED_POLLER, &esm.provisioned_poller_config),
        (LOGGING, &esm.logging_config),
    ] {
        if let Some(v) = value {
            out.insert(key.to_string(), v.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const MSK: &str =
        "arn:aws:kafka:us-east-1:123456789012:cluster/c/0b1c2d3e-1111-2222-3333-444455556666-1";
    const QUEUE: &str = "arn:aws:sqs:us-east-1:123456789012:q";

    fn mapping(body: &Value) -> EventSourceMapping {
        let configs = create_configs(body, "uuid-1").unwrap();
        let mut esm: EventSourceMapping = serde_json::from_value(json!({
            "uuid": "uuid-1",
            "function_arn": "arn:aws:lambda:us-east-1:123456789012:function:f",
            "event_source_arn": body["EventSourceArn"].as_str().unwrap_or_default(),
            "batch_size": 100,
            "enabled": true,
            "state": "Enabled",
            "last_modified": "2026-11-16T00:00:00Z",
        }))
        .unwrap();
        esm.self_managed_event_source = body.get("SelfManagedEventSource").cloned();
        esm.amazon_managed_kafka_event_source_config = configs.amazon_managed_kafka;
        esm.self_managed_kafka_event_source_config = configs.self_managed_kafka;
        esm.scaling_config = configs.scaling;
        esm.provisioned_poller_config = configs.provisioned_poller;
        esm.logging_config = configs.logging;
        esm
    }

    fn rejected(body: Value) -> String {
        validate_shapes(&body)
            .and_then(|()| create_configs(&body, "u").map(|_| ()))
            .expect_err(&format!("{body} was accepted"))
            .message()
            .to_string()
    }

    #[test]
    fn msk_consumer_group_defaults_to_the_mapping_uuid() {
        let esm = mapping(&json!({"EventSourceArn": MSK}));
        assert_eq!(
            esm.amazon_managed_kafka_event_source_config,
            Some(json!({"ConsumerGroupId": "uuid-1"}))
        );
        let esm = mapping(&json!({
            "EventSourceArn": MSK,
            "AmazonManagedKafkaEventSourceConfig": {
                "ConsumerGroupId": "orders group",
                "SchemaRegistryConfig": {"SchemaRegistryURI": "https://registry:8081", "EventRecordFormat": "JSON"},
            },
        }));
        assert_eq!(
            esm.amazon_managed_kafka_event_source_config.unwrap()["ConsumerGroupId"],
            "orders group"
        );
        // A self-managed cluster gets the same default.
        let esm = mapping(&json!({
            "SelfManagedEventSource": {"Endpoints": {"KAFKA_BOOTSTRAP_SERVERS": ["b:9092"]}},
        }));
        assert_eq!(
            esm.self_managed_kafka_event_source_config,
            Some(json!({"ConsumerGroupId": "uuid-1"}))
        );
        // Other sources carry no Kafka config.
        let esm = mapping(&json!({"EventSourceArn": QUEUE}));
        assert!(esm.amazon_managed_kafka_event_source_config.is_none());
        assert!(esm.self_managed_kafka_event_source_config.is_none());
    }

    #[test]
    fn kafka_members_are_checked_against_the_model() {
        for (cfg, member) in [
            (json!({"ConsumerGroupId": ""}), "consumerGroupId"),
            (
                json!({"ConsumerGroupId": "x".repeat(201)}),
                "consumerGroupId",
            ),
            (json!({"ConsumerGroupId": "bad#group"}), "consumerGroupId"),
            (
                json!({"SchemaRegistryConfig": {"SchemaRegistryURI": ""}}),
                "schemaRegistryConfig.schemaRegistryURI",
            ),
            (
                json!({"SchemaRegistryConfig": {"SchemaRegistryURI": "has space"}}),
                "schemaRegistryConfig.schemaRegistryURI",
            ),
            (
                json!({"SchemaRegistryConfig": {"EventRecordFormat": "AVRO"}}),
                "schemaRegistryConfig.eventRecordFormat",
            ),
            (
                json!({"SchemaRegistryConfig": {"AccessConfigs": [{"Type": "PASSWORD"}]}}),
                "schemaRegistryConfig.accessConfigs.1.type",
            ),
            (
                json!({"SchemaRegistryConfig": {"AccessConfigs": [{"Type": "BASIC_AUTH", "URI": "secret"}]}}),
                "schemaRegistryConfig.accessConfigs.1.uRI",
            ),
            (
                json!({"SchemaRegistryConfig": {"SchemaValidationConfigs": [{"Attribute": "HEADER"}]}}),
                "schemaRegistryConfig.schemaValidationConfigs.1.attribute",
            ),
        ] {
            let message = rejected(json!({
                "EventSourceArn": MSK,
                "AmazonManagedKafkaEventSourceConfig": cfg,
            }));
            assert!(
                message.contains(&format!("'amazonManagedKafkaEventSourceConfig.{member}'")),
                "{message}"
            );
        }
        // A complete, valid config is accepted.
        let body = json!({
            "EventSourceArn": MSK,
            "AmazonManagedKafkaEventSourceConfig": {
                "ConsumerGroupId": "g-1:a/b*c+d=e.f@g",
                "SchemaRegistryConfig": {
                    "SchemaRegistryURI": "arn:aws:glue:us-east-1:123456789012:registry/r",
                    "EventRecordFormat": "SOURCE",
                    "AccessConfigs": [{"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:123456789012:secret:s"}],
                    "SchemaValidationConfigs": [{"Attribute": "KEY"}, {"Attribute": "VALUE"}],
                },
            },
        });
        validate_shapes(&body).unwrap();
        create_configs(&body, "u").unwrap();
    }

    #[test]
    fn settings_are_refused_on_sources_that_do_not_take_them() {
        assert!(rejected(json!({
            "EventSourceArn": QUEUE,
            "AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "g"},
        }))
        .contains("Amazon MSK"));
        assert!(rejected(json!({
            "EventSourceArn": MSK,
            "SelfManagedKafkaEventSourceConfig": {"ConsumerGroupId": "g"},
        }))
        .contains("self-managed"));
        assert!(rejected(json!({
            "EventSourceArn": MSK,
            "ScalingConfig": {"MaximumConcurrency": 5},
        }))
        .contains("Amazon SQS"));
        assert!(rejected(json!({
            "EventSourceArn": QUEUE,
            "LoggingConfig": {"SystemLogLevel": "INFO"},
        }))
        .contains("Kafka"));
        assert!(rejected(json!({
            "EventSourceArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
            "ProvisionedPollerConfig": {"MaximumPollers": 5},
        }))
        .contains("ProvisionedPollerConfig"));
    }

    #[test]
    fn provisioned_pollers_get_their_documented_defaults_and_bounds() {
        let esm = mapping(
            &json!({"EventSourceArn": MSK, "ProvisionedPollerConfig": {"MaximumPollers": 123}}),
        );
        assert_eq!(
            esm.provisioned_poller_config,
            Some(json!({"MinimumPollers": 1, "MaximumPollers": 123}))
        );
        let esm = mapping(
            &json!({"EventSourceArn": QUEUE, "ProvisionedPollerConfig": {"MinimumPollers": 5}}),
        );
        assert_eq!(
            esm.provisioned_poller_config,
            Some(json!({"MinimumPollers": 5, "MaximumPollers": 200}))
        );
        rejected(
            json!({"EventSourceArn": QUEUE, "ProvisionedPollerConfig": {"MinimumPollers": 1}}),
        );
        rejected(
            json!({"EventSourceArn": MSK, "ProvisionedPollerConfig": {"MaximumPollers": 2001}}),
        );
        rejected(
            json!({"EventSourceArn": MSK, "ProvisionedPollerConfig": {"MinimumPollers": 20, "MaximumPollers": 10}}),
        );
        rejected(
            json!({"EventSourceArn": MSK, "ProvisionedPollerConfig": {"MinimumPollers": 201}}),
        );
        rejected(
            json!({"EventSourceArn": MSK, "ProvisionedPollerConfig": {"PollerGroupName": "bad name"}}),
        );
        rejected(
            json!({"EventSourceArn": QUEUE, "ProvisionedPollerConfig": {"PollerGroupName": "g"}}),
        );
        rejected(json!({"EventSourceArn": QUEUE, "ScalingConfig": {"MaximumConcurrency": 1}}));
        rejected(json!({"EventSourceArn": QUEUE, "ScalingConfig": {"MaximumConcurrency": 1001}}));
        rejected(json!({"EventSourceArn": MSK, "LoggingConfig": {"SystemLogLevel": "TRACE"}}));
    }

    #[test]
    fn update_keeps_the_consumer_group_and_replaces_the_rest() {
        let mut esm = mapping(&json!({
            "EventSourceArn": MSK,
            "AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "g1"},
            "ProvisionedPollerConfig": {"MaximumPollers": 10},
        }));
        let err = apply_update(
            &mut esm,
            &json!({
                "AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "g2"},
                "LoggingConfig": {"SystemLogLevel": "WARN"},
            }),
        )
        .unwrap_err();
        assert_eq!(err.code(), "InvalidParameterValueException");
        // The refused request changed nothing.
        assert!(esm.logging_config.is_none());

        apply_update(
            &mut esm,
            &json!({
                "AmazonManagedKafkaEventSourceConfig": {
                    "ConsumerGroupId": "g1",
                    "SchemaRegistryConfig": {"SchemaRegistryURI": "https://r", "EventRecordFormat": "JSON"},
                },
                "LoggingConfig": {"SystemLogLevel": "WARN"},
                "ProvisionedPollerConfig": {"MinimumPollers": 2, "MaximumPollers": 20},
            }),
        )
        .unwrap();
        assert_eq!(
            esm.amazon_managed_kafka_event_source_config,
            Some(json!({
                "ConsumerGroupId": "g1",
                "SchemaRegistryConfig": {"SchemaRegistryURI": "https://r", "EventRecordFormat": "JSON"},
            }))
        );
        assert_eq!(esm.logging_config, Some(json!({"SystemLogLevel": "WARN"})));
        assert_eq!(
            esm.provisioned_poller_config,
            Some(json!({"MinimumPollers": 2, "MaximumPollers": 20}))
        );

        // Empty structures remove the setting (an empty SchemaRegistryConfig
        // removes the registry, keeping the consumer group).
        apply_update(
            &mut esm,
            &json!({
                "AmazonManagedKafkaEventSourceConfig": {"SchemaRegistryConfig": {}},
                "ProvisionedPollerConfig": {},
                "LoggingConfig": {},
            }),
        )
        .unwrap();
        assert_eq!(
            esm.amazon_managed_kafka_event_source_config,
            Some(json!({"ConsumerGroupId": "g1"}))
        );
        assert!(esm.provisioned_poller_config.is_none());
        assert!(esm.logging_config.is_none());
    }

    #[test]
    fn constraint_violations_are_validation_exceptions() {
        let body = json!({
            "EventSourceArn": MSK,
            "AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "bad#group"},
        });
        let err = validate_shapes(&body).unwrap_err();
        assert_eq!(err.code(), "ValidationException");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().starts_with(
            "1 validation error detected: Value 'bad#group' at 'amazonManagedKafkaEventSourceConfig.consumerGroupId'"
        ));
        // A setting on the wrong source is a parameter error, not a shape one.
        let err = create_configs(
            &json!({"EventSourceArn": QUEUE, "LoggingConfig": {"SystemLogLevel": "INFO"}}),
            "u",
        )
        .unwrap_err();
        assert_eq!(err.code(), "InvalidParameterValueException");
    }

    #[test]
    fn a_mapping_stored_without_its_kafka_config_adopts_the_resent_group() {
        let mut esm = mapping(&json!({"EventSourceArn": MSK}));
        // As persisted before the config was recorded.
        esm.amazon_managed_kafka_event_source_config = None;
        apply_update(
            &mut esm,
            &json!({"AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "legacy-group"}}),
        )
        .unwrap();
        assert_eq!(
            esm.amazon_managed_kafka_event_source_config,
            Some(json!({"ConsumerGroupId": "legacy-group"}))
        );
        // From then on it is fixed.
        assert!(apply_update(
            &mut esm,
            &json!({"AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "other"}}),
        )
        .is_err());
    }

    #[test]
    fn tags_read_both_the_api_map_and_the_cloudformation_list() {
        assert_eq!(
            tags(&json!({"Tags": {"team": "data", "n": 1}})),
            [("team".to_string(), "data".to_string())].into()
        );
        assert_eq!(
            tags(&json!({"Tags": [{"Key": "team", "Value": "data"}, {"Key": "bad"}]})),
            [("team".to_string(), "data".to_string())].into()
        );
        assert!(tags(&json!({})).is_empty());
    }

    #[test]
    fn self_managed_consumption_mode_is_fixed_at_creation() {
        let mut esm = mapping(&json!({
            "SelfManagedEventSource": {"Endpoints": {"KAFKA_BOOTSTRAP_SERVERS": ["b:9092"]}},
            "SelfManagedKafkaEventSourceConfig": {"ConsumerGroupId": "g"},
        }));
        apply_update(
            &mut esm,
            &json!({"SelfManagedKafkaEventSourceConfig": {"ConsumptionMode": "Stream"}}),
        )
        .unwrap();
        assert!(apply_update(
            &mut esm,
            &json!({"SelfManagedKafkaEventSourceConfig": {"ConsumptionMode": "Queue"}}),
        )
        .is_err());
    }

    #[test]
    fn sqs_scaling_config_is_replaced_and_removed() {
        let mut esm =
            mapping(&json!({"EventSourceArn": QUEUE, "ScalingConfig": {"MaximumConcurrency": 10}}));
        assert_eq!(esm.scaling_config, Some(json!({"MaximumConcurrency": 10})));
        apply_update(
            &mut esm,
            &json!({"ScalingConfig": {"MaximumConcurrency": 15}}),
        )
        .unwrap();
        assert_eq!(esm.scaling_config, Some(json!({"MaximumConcurrency": 15})));
        apply_update(&mut esm, &json!({"BatchSize": 5})).unwrap();
        assert_eq!(esm.scaling_config, Some(json!({"MaximumConcurrency": 15})));
        apply_update(&mut esm, &json!({"ScalingConfig": {}})).unwrap();
        assert!(esm.scaling_config.is_none());
    }
}
