//! Amazon SageMaker (`sagemaker`) awsJson1.1 control-plane service for fakecloud.
//!
//! The full ~403-operation Amazon SageMaker Smithy model (SDK id `SageMaker`,
//! SigV4 signing name `sagemaker`, awsJson1.1 protocol). Every request is a
//! `POST /` whose operation is selected by the `X-Amz-Target: SageMaker.<Op>`
//! header, with all inputs carried in the JSON body (no HTTP path / label /
//! query bindings). The operation table, per-operation input constraints,
//! output member shapes, list element shapes, resource families and identifier
//! members are generated from the Smithy model (see `src/generated.rs`,
//! produced by `scripts/generate-sagemaker-tables.py`), so the control plane
//! tracks the model exactly.
//!
//! **What is real.** Every named resource family — models, endpoints, endpoint
//! configs, training / processing / transform / labeling / compilation / AutoML
//! / hyper-parameter-tuning jobs, notebook instances (+ lifecycle configs),
//! model packages (+ groups), pipelines, feature groups, domains, user
//! profiles, spaces, apps, images, experiments, trials, actions, artifacts,
//! contexts, clusters, inference components, monitoring schedules, workteams,
//! workforces, and the rest — mints a proper ARN, persists its accepted input
//! attributes plus creation / last-modified timestamps, and echoes them back
//! on `Describe*` / `List*`. Create is conflict-checked (`ResourceInUse`),
//! Describe / Update / Delete round-trip by the resource's identifier (name,
//! id, or ARN), and `AddTags` / `ListTags` / `DeleteTags` persist tags keyed by
//! ARN. State is account-partitioned and persisted across restarts. Input
//! validation is model-derived: required members, string `@length`, numeric
//! `@range`, and `@enum` constraints are enforced, returning SageMaker's
//! `ValidationException` / `ResourceNotFound` / `ResourceInUse`.
//!
//! **Honest emulation choices (documented, not stubbed):**
//! * This is the SageMaker **control plane** only. There is no ML execution
//!   plane: training / processing / transform / AutoML / tuning jobs are
//!   created, persisted and described, but no container is scheduled, no model
//!   is trained, and no inference endpoint serves traffic. Jobs and endpoints
//!   are not advanced through a live lifecycle by a background scheduler.
//! * Timestamps are emitted as awsJson1.1 epoch-second JSON numbers.
//! * A handful of resources whose `Describe*` identifier is a service-minted
//!   Id / ARN distinct from the create-time Name (e.g. `Domain`, `ImageVersion`,
//!   `ModelCardExportJob`) resolve by scanning the family's minted identifiers,
//!   so a describe by the returned Id / ARN still round-trips.

pub mod generated;
pub mod persistence;
pub mod service;
pub mod state;
pub mod validate;

pub use service::{SageMakerService, SAGEMAKER_ACTIONS};
pub use state::{
    SageMakerData, SageMakerSnapshot, SharedSageMakerState, SAGEMAKER_SNAPSHOT_SCHEMA_VERSION,
};

/// Start a SageMaker pipeline execution from a cross-service delivery (an
/// EventBridge Scheduler `sagemaker:pipeline` target). Persists a
/// `PipelineExecution` record keyed by its minted ARN so
/// DescribePipelineExecution / ListPipelineExecutions resolve it — the same
/// shape the `StartPipelineExecution` API produces. `pipeline_arn` is
/// `arn:aws:sagemaker:<region>:<account>:pipeline/<name>`; `parameters` is the
/// target's `PipelineParameterList` (a JSON array, echoed onto the record).
pub fn start_pipeline_execution_from_delivery(
    state: &SharedSageMakerState,
    pipeline_arn: &str,
    parameters: &serde_json::Value,
) {
    let parts: Vec<&str> = pipeline_arn.split(':').collect();
    let region = parts.get(3).copied().unwrap_or("us-east-1");
    let account = parts.get(4).copied().unwrap_or_default();
    let pipeline_name = pipeline_arn
        .rsplit_once("pipeline/")
        .map(|(_, n)| n)
        .unwrap_or_default();
    if account.is_empty() || pipeline_name.is_empty() {
        tracing::warn!(%pipeline_arn, "scheduler->sagemaker: malformed pipeline ARN, skipping");
        return;
    }

    let mut g = state.write();
    let data = g.get_or_create(account);
    let exec_id = service::mint_id(account, "PipelineExecution", &data.next_seq().to_string());
    let exec_arn = format!(
        "arn:aws:sagemaker:{region}:{account}:pipeline/{pipeline_name}/execution/{exec_id}"
    );
    let mut record = serde_json::Map::new();
    record.insert(
        "PipelineExecutionArn".to_string(),
        serde_json::Value::String(exec_arn.clone()),
    );
    record.insert(
        "PipelineArn".to_string(),
        serde_json::Value::String(pipeline_arn.to_string()),
    );
    record.insert(
        "PipelineExecutionStatus".to_string(),
        serde_json::Value::String("Executing".to_string()),
    );
    record.insert("StartTime".to_string(), service::now_epoch());
    if parameters.is_array() {
        record.insert("PipelineParameters".to_string(), parameters.clone());
    }
    data.put_resource(
        "PipelineExecution",
        &exec_arn,
        serde_json::Value::Object(record),
    );
}
