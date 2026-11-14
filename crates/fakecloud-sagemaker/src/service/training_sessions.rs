//! LoRA fine-tuning training sessions (`CreateTrainingSession`,
//! `DescribeTrainingSession`, `StopTrainingSession`, `DeleteTrainingSession`;
//! `ListTrainingSessions` is served by the generic list engine from the stored
//! `TrainingSessionStatus`).
//!
//! A session is created `Created` (its compute is provisioned at once; no
//! training operation has been submitted through the Training Session Runtime
//! API, so it reports zero token usage). `StopTrainingSession` settles it to
//! `Stopped` with an `EndTime`; a session that has already ended cannot be
//! stopped again. `DeleteTrainingSession` requires a stopped (or otherwise
//! ended) session, as the API documents, and removes it with its tags.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::OpMeta;
use crate::state::SageMakerData;
use crate::validate::invalid;

use super::special::str_member;
use super::{engine, in_use, not_found, now_epoch, ok_json, Ctx, SageMakerService};

pub(super) const FAMILY: &str = "TrainingSession";
const STATUS: &str = "TrainingSessionStatus";

/// Statuses of a session that is still holding compute: it must be stopped
/// before it can be deleted, and it can be stopped.
const ACTIVE: &[&str] = &["Created", "Scheduled", "Blocked", "Running", "Stopping"];

fn int_in(
    v: Option<&Value>,
    member: &str,
    min: i64,
    max: Option<i64>,
) -> Result<(), AwsServiceError> {
    let Some(n) = v.and_then(Value::as_i64) else {
        return Err(invalid(format!(
            "{member} is required and must be an integer."
        )));
    };
    if n < min || max.is_some_and(|m| n > m) {
        let bound = match max {
            Some(m) => format!("between {min} and {m}"),
            None => format!("greater than or equal to {min}"),
        };
        return Err(invalid(format!("{member} must be {bound}.")));
    }
    Ok(())
}

fn str_len(
    v: Option<&Value>,
    member: &str,
    required: bool,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    match v.filter(|v| !v.is_null()) {
        None if required => Err(invalid(format!("{member} is required."))),
        None => Ok(()),
        Some(Value::String(s)) if (min..=max).contains(&s.chars().count()) => Ok(()),
        Some(Value::String(_)) => Err(invalid(format!(
            "{member} must have length between {min} and {max}."
        ))),
        Some(_) => Err(invalid(format!("{member} must be a string."))),
    }
}

fn string_list(
    v: Option<&Value>,
    member: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    let Some(items) = v.and_then(Value::as_array) else {
        return Err(invalid(format!("{member} is required.")));
    };
    if !(min..=max).contains(&items.len()) {
        return Err(invalid(format!(
            "{member} must contain between {min} and {max} items."
        )));
    }
    for item in items {
        str_len(Some(item), member, true, 0, 32)?;
    }
    Ok(())
}

/// The nested constraints of the create request's structures (the generated
/// rules cover only the top-level members).
fn validate_create(body: &Map<String, Value>) -> Result<(), AwsServiceError> {
    let lora = body.get("LoraConfig").and_then(Value::as_object);
    let lora = lora.ok_or_else(|| invalid("LoraConfig must be a structure."))?;
    int_in(lora.get("Rank"), "LoraConfig.Rank", 1, Some(256))?;
    int_in(lora.get("Alpha"), "LoraConfig.Alpha", 1, None)?;

    let model = body.get("ModelConfig").and_then(Value::as_object);
    let model = model.ok_or_else(|| invalid("ModelConfig must be a structure."))?;
    str_len(
        model.get("BaseModelArn"),
        "ModelConfig.BaseModelArn",
        true,
        1,
        2048,
    )?;
    if model
        .get("AcceptEula")
        .is_some_and(|v| !v.is_boolean() && !v.is_null())
    {
        return Err(invalid("ModelConfig.AcceptEula must be a boolean."));
    }

    let output = body.get("OutputConfig").and_then(Value::as_object);
    let output = output.ok_or_else(|| invalid("OutputConfig must be a structure."))?;
    let s3 = output.get("S3Location").and_then(Value::as_object);
    let s3 = s3.ok_or_else(|| invalid("OutputConfig.S3Location is required."))?;
    str_len(
        s3.get("Path"),
        "OutputConfig.S3Location.Path",
        true,
        1,
        1024,
    )?;
    str_len(
        s3.get("KmsKeyArn"),
        "OutputConfig.S3Location.KmsKeyArn",
        false,
        1,
        2048,
    )?;

    if let Some(vpc) = body.get("VpcConfig").filter(|v| !v.is_null()) {
        let vpc = vpc
            .as_object()
            .ok_or_else(|| invalid("VpcConfig must be a structure."))?;
        string_list(
            vpc.get("SecurityGroupIds"),
            "VpcConfig.SecurityGroupIds",
            1,
            5,
        )?;
        string_list(vpc.get("Subnets"), "VpcConfig.Subnets", 1, 16)?;
    }
    Ok(())
}

fn resolve(data: Option<&SageMakerData>, name: &str) -> Result<String, AwsServiceError> {
    data.and_then(|d| d.resolve_key(FAMILY, name))
        .ok_or_else(|| not_found(format!("Training session '{name}' does not exist.")))
}

fn status_of(record: &Value) -> &str {
    record
        .get(STATUS)
        .and_then(Value::as_str)
        .unwrap_or("Created")
}

/// `CreateTrainingSession`: validate the nested configuration, require the
/// named model package group to exist, and store the session `Created`.
pub(super) fn create(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    validate_create(body)?;
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(group) = body.get("ModelPackageGroupArn").and_then(Value::as_str) {
        if data.resolve_key("ModelPackageGroup", group).is_none() {
            return Err(not_found(format!(
                "Model package group '{group}' does not exist."
            )));
        }
    }
    let mut seed = body.clone();
    seed.insert(STATUS.to_string(), Value::String("Created".to_string()));
    seed.insert(
        "BillableTokenUsage".to_string(),
        json!({ "PrefillTokenCount": 0, "SampleTokenCount": 0, "TrainingTokenCount": 0 }),
    );
    Ok((engine::create(data, ctx, meta, &seed)?, true))
}

/// `DescribeTrainingSession`: the whole stored session (with its tags) inside
/// `TrainingSessionDetail`; dispatch projects it onto the detail shape.
pub(super) fn describe(
    svc: &SageMakerService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = str_member(body, "TrainingSessionName");
    let g = svc.state.read();
    let data = g.get(&ctx.account);
    let key = resolve(data, &name)?;
    let data = data.expect("resolved above");
    let record = data
        .get_resource(FAMILY, &key)
        .map(|r| data.record_with_tags(FAMILY, r))
        .unwrap_or(Value::Null);
    let mut out = Map::new();
    out.insert("TrainingSessionDetail".to_string(), record);
    Ok((ok_json(Value::Object(out)), false))
}

/// `StopTrainingSession`: an active session settles to `Stopped` and records
/// its `EndTime`. Stopping a session that has already ended is rejected.
pub(super) fn stop(
    svc: &SageMakerService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = str_member(body, "TrainingSessionName");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let key = resolve(Some(data), &name)?;
    let record = data
        .get_resource_mut(FAMILY, &key)
        .and_then(Value::as_object_mut)
        .ok_or_else(|| not_found(format!("Training session '{name}' does not exist.")))?;
    let status = status_of(&Value::Object(record.clone())).to_string();
    if !ACTIVE.contains(&status.as_str()) {
        return Err(invalid(format!(
            "Training session '{name}' cannot be stopped in status {status}."
        )));
    }
    let now = now_epoch();
    record.insert(STATUS.to_string(), Value::String("Stopped".to_string()));
    record.insert("EndTime".to_string(), now.clone());
    record.insert("LastModifiedTime".to_string(), now);
    Ok((ok_json(Value::Object(Map::new())), true))
}

/// `DeleteTrainingSession`: an ended session is removed with its tags; a
/// session still holding compute must be stopped first.
pub(super) fn delete(
    svc: &SageMakerService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = str_member(body, "TrainingSessionName");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let key = resolve(Some(data), &name)?;
    let status = data
        .get_resource(FAMILY, &key)
        .map(|r| status_of(r).to_string())
        .unwrap_or_default();
    if ACTIVE.contains(&status.as_str()) {
        return Err(in_use(format!(
            "Training session '{name}' is {status}; stop it before deleting it."
        )));
    }
    data.remove_resource(FAMILY, &key);
    Ok((ok_json(Value::Object(Map::new())), true))
}
