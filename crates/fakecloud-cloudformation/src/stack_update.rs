//! The stack update engine shared by `UpdateStack` and `ExecuteChangeSet`.
//!
//! The order and every status and event below mirror what CloudFormation was
//! recorded doing on a real account:
//!
//! 1. Validate. A name another resource holds, or an export name another stack
//!    owns, fails the update before any resource is touched.
//! 2. Create, update and replace, in dependency order, recording an undo log. A
//!    replacement creates the new physical resource and leaves the old one
//!    alone. A resource removed from the template is not touched yet.
//! 3. On failure, walk the log backwards: updated resources get their previous
//!    definition back, replaced ones point at the old physical resource again.
//!    Then `*_CLEANUP_IN_PROGRESS` deletes what the update created.
//! 4. On success, commit the template, then `*_CLEANUP_IN_PROGRESS` deletes the
//!    removed resources and the replaced old ones, as their `DeletionPolicy` and
//!    `UpdateReplacePolicy` allow.
//!
//! Provisioning runs with the CloudFormation state lock released: a nested
//! stack takes that lock itself. The stack's `*_IN_PROGRESS` status is what keeps
//! a second operation off the stack meanwhile.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::replacement;
use crate::resource_provisioner::{policy_retains_physical, ResourceProvisioner};
use crate::service::{
    collect_nested_touched_types, merged_resource_attributes, record_event_with_reason,
    CloudFormationService,
};
use crate::state::{
    CloudFormationState, RegionalAccounts, SharedCloudFormationState, Stack, StackOutput,
    StackResource,
};
use crate::template::{self, ParsedTemplate, ResourceDefinition};

/// Points tests can act at between engine steps, where another request or a crash
/// could land: `undo:<logical id>` before a resource's re-apply, `cleanup:<logical
/// id>` before a cleanup delete.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::RefCell;

    type Hook = Box<dyn FnMut(&str)>;

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(crate) fn set(hook: impl FnMut(&str) + 'static) {
        HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    }

    pub(crate) fn clear() {
        HOOK.with(|slot| *slot.borrow_mut() = None);
    }

    pub(crate) fn fire(point: &str) {
        // Out of its slot while it runs, so it can call back into the engine.
        let hook = HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(mut hook) = hook {
            hook(point);
            HOOK.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.is_none() {
                    *slot = Some(hook);
                }
            });
        }
    }
}

const STACK_TYPE: &str = "AWS::CloudFormation::Stack";
const UPDATING: bool = true;
const REPLACEMENT_REASON: &str =
    "Requested update requires the creation of a new physical resource; hence creating one.";
const CREATE_INITIATED_REASON: &str = "Resource creation Initiated";
pub(crate) const USER_INITIATED_REASON: &str = "User Initiated";
pub(crate) const USER_INITIATED_ROLLBACK_REASON: &str = "User Initiated Rollback";
const VALIDATION_FAILED_SUMMARY: &str = "Validation failed with 1 error(s). Call DescribeEvents to retrieve the full list of issues with resource and property details, resolve each error, then retry the operation.";
const VALIDATION_FAILURE_REASON: &str =
    "Validation failure detected. See operation's FAILED event for details.";

/// Types whose same-identity replacement CloudFormation does not refuse up front:
/// it asks the resource handler for the new resource, and the handler refuses.
/// Recorded for `AWS::SQS::Queue`; `AWS::DynamoDB::Table` gets the custom-name
/// failure instead, and nothing in the registry tells the two classes apart.
/// `AWS::KinesisFirehose::DeliveryStream` is unrecorded and placed here as a
/// handler-provisioned type like SQS.
const HANDLER_REFUSES_SAME_IDENTITY: &[(&str, Option<&str>)] = &[
    (
        "AWS::SQS::Queue",
        Some("Queue creation failed because the queue already exists"),
    ),
    ("AWS::KinesisFirehose::DeliveryStream", None),
];

/// Whether a stack in `status` accepts a stack update.
pub(crate) fn status_allows_update(status: &str) -> bool {
    matches!(
        status,
        "CREATE_COMPLETE"
            | "UPDATE_COMPLETE"
            | "UPDATE_ROLLBACK_COMPLETE"
            | "UPDATE_FAILED"
            | "IMPORT_COMPLETE"
            | "IMPORT_ROLLBACK_COMPLETE"
    )
}

/// The refusal for an update of a stack in a state that does not allow it.
pub(crate) fn cannot_update_message(stack_id: &str, status: &str) -> String {
    format!("Stack:{stack_id} is in {status} state and can not be updated.")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    /// A `CREATE` change set executing against a `REVIEW_IN_PROGRESS` stack.
    Create,
    Update,
}

impl OperationKind {
    fn in_progress(self) -> &'static str {
        match self {
            Self::Create => "CREATE_IN_PROGRESS",
            Self::Update => "UPDATE_IN_PROGRESS",
        }
    }
    fn complete(self) -> &'static str {
        match self {
            Self::Create => "CREATE_COMPLETE",
            Self::Update => "UPDATE_COMPLETE",
        }
    }
    fn failed(self) -> &'static str {
        match self {
            Self::Create => "CREATE_FAILED",
            Self::Update => "UPDATE_FAILED",
        }
    }
    fn rollback_in_progress(self) -> &'static str {
        match self {
            Self::Create => "ROLLBACK_IN_PROGRESS",
            Self::Update => "UPDATE_ROLLBACK_IN_PROGRESS",
        }
    }
    fn rollback_complete(self) -> &'static str {
        match self {
            Self::Create => "ROLLBACK_COMPLETE",
            Self::Update => "UPDATE_ROLLBACK_COMPLETE",
        }
    }
    fn rollback_failed(self) -> &'static str {
        match self {
            Self::Create => "ROLLBACK_FAILED",
            Self::Update => "UPDATE_ROLLBACK_FAILED",
        }
    }
}

/// The stack as it stood before the operation: what a rollback restores.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackBaseline {
    template: String,
    parameters: BTreeMap<String, String>,
    tags: BTreeMap<String, String>,
    description: Option<String>,
    notification_arns: Vec<String>,
    outputs: Vec<StackOutput>,
    resources: Vec<StackResource>,
    imported_names: Vec<String>,
}

/// A resolved definition an undo re-applies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedDefinition {
    properties: Value,
    deletion_policy: Option<String>,
    update_replace_policy: Option<String>,
    #[serde(default)]
    resource_type: String,
}

impl SavedDefinition {
    fn of(def: &ResourceDefinition) -> Self {
        Self {
            properties: def.properties.clone(),
            deletion_policy: def.deletion_policy.clone(),
            update_replace_policy: def.update_replace_policy.clone(),
            resource_type: def.resource_type.clone(),
        }
    }

    fn to_definition(&self, logical_id: &str, resource_type: &str) -> ResourceDefinition {
        ResourceDefinition {
            logical_id: logical_id.to_string(),
            resource_type: resource_type.to_string(),
            properties: self.properties.clone(),
            deletion_policy: self.deletion_policy.clone(),
            update_replace_policy: self.update_replace_policy.clone(),
        }
    }
}

/// One step of the undo log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEntry {
    /// Added by this update.
    Created { resource: StackResource },
    /// Failed to create: only a `DELETE_COMPLETE` event remains to record.
    CreateFailed {
        logical_id: String,
        resource_type: String,
    },
    /// Updated in place. `after` is what a re-apply updates from.
    Updated {
        before: StackResource,
        after: StackResource,
        previous: Option<SavedDefinition>,
    },
    /// An update that failed. `previous` is set when the failure may have left
    /// the resource changed (a re-provision that deleted it first), so the undo
    /// has to apply the old definition; otherwise nothing changed.
    UpdateFailed {
        before: StackResource,
        previous: Option<SavedDefinition>,
    },
    /// A new physical resource was created in place of `before`.
    Replaced {
        before: StackResource,
        new: StackResource,
    },
    /// The new physical resource of a replacement could not be created.
    ReplaceFailed { before: StackResource },
    /// A nested stack updated in place. Its own journal stays on the child
    /// stack until the parent finishes or rolls back.
    NestedUpdated {
        before: StackResource,
        child_stack_id: String,
        /// The child stopped at `UPDATE_FAILED` (`DisableRollback`), so a retry
        /// must try it again.
        #[serde(default)]
        failed: bool,
    },
}

impl JournalEntry {
    /// The logical id a retried update must attempt again even when its
    /// definition matches the template (only after `DisableRollback`).
    fn retried_logical_id(&self) -> Option<&str> {
        match self {
            Self::UpdateFailed { before, .. }
            | Self::ReplaceFailed { before }
            | Self::NestedUpdated {
                before,
                failed: true,
                ..
            } => Some(&before.logical_id),
            _ => None,
        }
    }
}

/// Something a rollback still has to delete in its cleanup phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RollbackDelete {
    /// Never came into existence: a bare `DELETE_COMPLETE` event.
    Failed {
        logical_id: String,
        resource_type: String,
    },
    Created {
        resource: StackResource,
        /// A handler heard of this resource, so it hears of the delete too.
        #[serde(default)]
        released: bool,
    },
    Replacement {
        resource: StackResource,
        #[serde(default)]
        released: bool,
    },
}

/// The undo log of an operation. Kept on the stack only while an operation
/// waits for a later call: after `DisableRollback` left it `UPDATE_FAILED`, after
/// `UPDATE_ROLLBACK_FAILED`, or on a nested stack whose parent is not done yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateJournal {
    kind: OperationKind,
    before: StackBaseline,
    entries: Vec<JournalEntry>,
    /// Removed from the template, deleted by a successful update's cleanup.
    #[serde(default)]
    removed: Vec<StackResource>,
    /// Filled while a rollback walks `entries`.
    #[serde(default)]
    rollback_deletes: Vec<RollbackDelete>,
    #[serde(default)]
    restored: BTreeMap<String, StackResource>,
    /// What each resource physically holds now, resolved. A held update's
    /// template is the attempted one, which says nothing about the resources it
    /// never reached, so this is what a retry compares against and what its undo
    /// goes back to.
    #[serde(default)]
    applied: BTreeMap<String, SavedDefinition>,
    /// How many leading `entries` an earlier attempt finished with, its stop at
    /// `UPDATE_FAILED` having run their custom-resource invokes. A handler has
    /// seen those and must hear about their undo; it has seen nothing of the
    /// rest, and a rollback of them stays silent.
    #[serde(default)]
    released_entries: usize,
    /// `RetainExceptOnCreate` of the rollback, which a rollback cleanup resumed
    /// after a restart still has to honor.
    #[serde(default)]
    retain_except_on_create: bool,
}

impl UpdateJournal {
    /// The journal of a `CreateStack` held at `CREATE_FAILED`: everything it made
    /// was created by this operation, and there is no earlier state.
    pub(crate) fn for_failed_create(stack: &Stack) -> Self {
        Self {
            kind: OperationKind::Create,
            before: StackBaseline {
                template: stack.template.clone(),
                parameters: stack.parameters.clone(),
                tags: stack.tags.clone(),
                description: stack.description.clone(),
                notification_arns: stack.notification_arns.clone(),
                outputs: Vec::new(),
                resources: Vec::new(),
                imported_names: Vec::new(),
            },
            entries: stack
                .resources
                .iter()
                .map(|resource| {
                    if resource.physical_id.is_empty() {
                        JournalEntry::CreateFailed {
                            logical_id: resource.logical_id.clone(),
                            resource_type: resource.resource_type.clone(),
                        }
                    } else {
                        JournalEntry::Created {
                            resource: resource.clone(),
                        }
                    }
                })
                .collect(),
            removed: Vec::new(),
            rollback_deletes: Vec::new(),
            restored: BTreeMap::new(),
            applied: BTreeMap::new(),
            released_entries: stack.resources.len(),
            retain_except_on_create: false,
        }
    }

    /// The nested stacks whose own journals wait on this one.
    fn nested_children(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().filter_map(|entry| match entry {
            JournalEntry::NestedUpdated { child_stack_id, .. } => Some(child_stack_id.as_str()),
            _ => None,
        })
    }

    /// Physical resources this journal knows about that the stack's resource
    /// list no longer does, which a stack delete must not leak.
    pub(crate) fn orphans(&self) -> Vec<StackResource> {
        let mut out = Vec::new();
        for entry in &self.entries {
            if let JournalEntry::Replaced { before, .. } = entry {
                out.push(before.clone());
            }
        }
        for delete in &self.rollback_deletes {
            if let RollbackDelete::Created { resource, .. }
            | RollbackDelete::Replacement { resource, .. } = delete
            {
                out.push(resource.clone());
            }
        }
        out
    }
}

/// Everything one update applies.
pub(crate) struct UpdateInput<'a> {
    pub kind: OperationKind,
    pub template_body: &'a str,
    pub parsed: &'a ParsedTemplate,
    pub parameters: &'a BTreeMap<String, String>,
    /// Empty keeps the stack's tags, as both callers document.
    pub tags: BTreeMap<String, String>,
    /// Empty keeps the stack's notification ARNs.
    pub notification_arns: Vec<String>,
    pub imports: &'a BTreeMap<String, String>,
    pub imported_names: Vec<String>,
    pub export_names: Vec<String>,
    pub disable_rollback: bool,
    pub retain_except_on_create: bool,
    /// A nested stack's update: stops after the commit, leaving the cleanup (or
    /// the rollback) to whichever way the parent goes.
    pub nested: bool,
}

/// How an operation ended.
#[derive(Debug)]
pub(crate) struct UpdateOutcome {
    /// The stack's status when the engine returned.
    pub status: String,
    pub succeeded: bool,
    /// Why the operation failed.
    pub reason: Option<String>,
    /// Types of every resource the operation touched, for persistence.
    pub touched_types: Vec<String>,
}

/// Options of a rollback, which `ContinueUpdateRollback` and `RollbackStack`
/// set differently from an update that failed on its own.
#[derive(Debug, Default, Clone)]
pub(crate) struct RollbackOptions {
    pub retain_except_on_create: bool,
    /// Logical ids whose rollback is skipped.
    pub skip: BTreeSet<String>,
    /// Skip every resource whose rollback fails.
    pub force: bool,
    pub nested: bool,
    /// `ContinueUpdateRollback`, which is an operation of its own kind.
    pub continued: bool,
}

/// A failed forward pass.
struct ForwardFailure {
    reason: String,
    /// `CancelUpdateStack`, which rolls back whatever `DisableRollback` says.
    cancelled: bool,
}

/// Run a provisioner call, turning a panic into an ordinary failure. Without
/// the state lock held there is nothing to poison, and a stack left forever in
/// `UPDATE_IN_PROGRESS` would refuse every later operation.
fn guarded<T>(call: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(call)) {
        Ok(result) => result,
        Err(_) => Err("Internal failure.".to_string()),
    }
}

/// A failure's event reason. A handler's own answer and CloudFormation's refusals
/// stand as they are, as recorded; `CloudFormation does not support...` only when
/// updating, since a create's goes through the handler.
fn provisioning_reason(message: &str, updating: bool) -> String {
    if message.starts_with("Received response status")
        || message.starts_with(CUSTOM_NO_RESPONSE_PREFIX)
        || message.starts_with("Modifying service token")
        || (updating && message.starts_with("CloudFormation does not support"))
    {
        message.to_string()
    } else {
        handler_reason(message)
    }
}

/// How CloudFormation's own failure for a custom resource that never
/// answered begins; it stands unwrapped, like a handler's answer.
pub(crate) const CUSTOM_NO_RESPONSE_PREFIX: &str =
    "CloudFormation did not receive a response from your Custom Resource.";

pub(crate) fn handler_reason(message: &str) -> String {
    // InvalidRequest is recorded for the provisioning-level refusals, and
    // AlreadyExists for a taken name.
    let code = if message.starts_with("CloudFormation does not support") {
        "InvalidRequest"
    } else if message.contains("already exists") {
        "AlreadyExists"
    } else {
        "GeneralServiceException"
    };
    handler_reason_with_code(message, code)
}

fn handler_reason_with_code(message: &str, code: &str) -> String {
    format!(
        "Resource handler returned message: \"{message}\" (RequestToken: {}, HandlerErrorCode: {code})",
        uuid::Uuid::new_v4()
    )
}

fn is_custom(resource_type: &str) -> bool {
    resource_type.starts_with("Custom::") || resource_type == "AWS::CloudFormation::CustomResource"
}

/// Whether `physical_id` (a name, an ARN or a URL) is the resource called `name`.
fn physical_id_names(physical_id: &str, name: &str) -> bool {
    physical_id == name
        || physical_id
            .strip_suffix(name)
            .is_some_and(|head| head.ends_with(['/', ':']))
}

/// The export of `names` another stack owns, as the reason a stack op fails.
pub(crate) fn export_conflict(
    state: &CloudFormationState,
    stack_name: &str,
    stack_id: &str,
    names: &[String],
) -> Option<String> {
    names.iter().find_map(|name| {
        let export = state.exports.get(name)?;
        let own = export.exporting_stack_name == stack_name
            || (!stack_id.is_empty() && export.exporting_stack_id == stack_id);
        (!own).then(|| {
            format!(
                "Export with name {name} is already exported by stack {}",
                export.exporting_stack_name
            )
        })
    })
}

/// Why an update cannot go ahead given who imports this stack's exports: the
/// exports it would drop or change while another stack uses them. `new_outputs`
/// is what the update would export; its values may not be final before
/// provisioning, so the commit asks again.
fn export_in_use(
    state: &CloudFormationState,
    stack_name: &str,
    new_outputs: &[(String, Option<String>)],
) -> Option<String> {
    for (name, export) in &state.exports {
        if export.exporting_stack_name != stack_name {
            continue;
        }
        let consumers: Vec<&str> = state
            .imports
            .get(name)
            .map(|stacks| {
                stacks
                    .iter()
                    .map(String::as_str)
                    .filter(|consumer| *consumer != stack_name)
                    .collect()
            })
            .unwrap_or_default();
        if consumers.is_empty() {
            continue;
        }
        let users = consumers.join(", ");
        match new_outputs.iter().find(|(new, _)| new == name) {
            None => {
                return Some(format!(
                    "Delete canceled. Cannot delete export {name} as it is in use by {users}."
                ))
            }
            Some((_, Some(value))) if *value != export.value => {
                return Some(format!(
                    "Update canceled. Cannot update export {name} as it is in use by {users}."
                ))
            }
            _ => {}
        }
    }
    None
}

/// The refusal for a template that changes a resource's type.
pub(crate) fn type_change_refusal(stack: &Stack, parsed: &ParsedTemplate) -> Option<String> {
    let changed: Vec<&str> = parsed
        .resources
        .iter()
        .filter(|def| {
            stack
                .resources
                .iter()
                .any(|r| r.logical_id == def.logical_id && r.resource_type != def.resource_type)
        })
        .map(|def| def.logical_id.as_str())
        .collect();
    (!changed.is_empty()).then(|| {
        format!(
            "Update of resource type is not permitted. The new template modifies resource type of the following resources: [{}]",
            changed.join(", ")
        )
    })
}

/// Whether an update would change nothing: no resource, policy, output, tag or
/// notification differs, which AWS refuses rather than running.
#[allow(clippy::too_many_arguments)]
pub(crate) fn update_changes_nothing(
    state: &SharedCloudFormationState,
    account_id: &str,
    region: &str,
    stack: &Stack,
    parsed: &ParsedTemplate,
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    imports: &BTreeMap<String, String>,
    tags: &BTreeMap<String, String>,
    notification_arns: &[String],
) -> bool {
    if stack.pending_update.is_some()
        || !change_set_rows(Some(stack), parsed, template_body, parameters, imports).is_empty()
    {
        return false;
    }
    let policies_same = parsed.resources.iter().all(|def| {
        stack.resources.iter().any(|r| {
            r.logical_id == def.logical_id
                && r.deletion_policy == def.deletion_policy
                && r.update_replace_policy == def.update_replace_policy
        })
    });
    let summary = |outputs: &[StackOutput]| {
        let mut rows: Vec<(String, String, Option<String>, Option<String>)> = outputs
            .iter()
            .map(|o| {
                (
                    o.key.clone(),
                    o.value.clone(),
                    o.export_name.clone(),
                    o.description.clone(),
                )
            })
            .collect();
        rows.sort();
        rows
    };
    let outputs = CloudFormationService::resolve_template_outputs(
        template_body,
        parameters,
        &stack.resources,
        state,
        account_id,
        region,
    );
    // Pseudo parameters follow from the stack and region, not the request.
    let set_parameters = |all: &BTreeMap<String, String>| -> BTreeMap<String, String> {
        all.iter()
            .filter(|(key, _)| !key.starts_with("AWS::"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    };
    policies_same
        && set_parameters(parameters) == set_parameters(&stack.parameters)
        && summary(&outputs) == summary(&stack.outputs)
        && (tags.is_empty() || *tags == stack.tags)
        && (notification_arns.is_empty() || notification_arns == stack.notification_arns)
        && parsed.description == stack.description
}

pub(crate) struct UpdateEngine<'a> {
    state: &'a SharedCloudFormationState,
    provisioner: &'a ResourceProvisioner,
    account_id: &'a str,
    region: &'a str,
    stack_id: String,
    stack_name: String,
    touched: RefCell<Vec<String>>,
    /// The open operation `DescribeEvents` files events under.
    operation: RefCell<Option<OpenOperation>>,
}

struct OpenOperation {
    id: String,
    kind: &'static str,
    started: String,
}

fn event_time() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

impl<'a> UpdateEngine<'a> {
    pub(crate) fn new(
        state: &'a SharedCloudFormationState,
        provisioner: &'a ResourceProvisioner,
        account_id: &'a str,
        region: &'a str,
        stack_id: &str,
        stack_name: &str,
    ) -> Self {
        Self {
            state,
            provisioner,
            account_id,
            region,
            stack_id: stack_id.to_string(),
            stack_name: stack_name.to_string(),
            touched: RefCell::new(Vec::new()),
            operation: RefCell::new(None),
        }
    }

    fn file_operation_event(&self, state: &mut CloudFormationState, mut event: Value) {
        let Some(operation) = &*self.operation.borrow() else {
            return;
        };
        event["EventId"] = json!(uuid::Uuid::new_v4().to_string());
        event["StackId"] = json!(self.stack_id);
        event["OperationId"] = json!(operation.id);
        let events = state
            .extras
            .entry("operation_events".to_string())
            .or_default();
        // Zero-padded, so the store's order is the order they happened in.
        events.insert(format!("{:012}", events.len()), event);
    }

    /// Start the operation later events are filed under.
    fn open_operation(&self, kind: &'static str) {
        let started = event_time();
        *self.operation.borrow_mut() = Some(OpenOperation {
            id: uuid::Uuid::new_v4().to_string(),
            kind,
            started: started.clone(),
        });
        self.with_state(|state| {
            self.file_operation_event(
                state,
                json!({
                    "OperationType": kind,
                    "OperationStatus": "IN_PROGRESS",
                    "EventType": "STACK_EVENT",
                    "Timestamp": started,
                    "StartTime": started,
                }),
            );
        });
    }

    fn close_operation(&self, status: &str) {
        let Some((kind, started)) = self
            .operation
            .borrow()
            .as_ref()
            .map(|op| (op.kind, op.started.clone()))
        else {
            return;
        };
        let now = event_time();
        self.with_state(|state| {
            self.file_operation_event(
                state,
                json!({
                    "OperationType": kind,
                    "OperationStatus": status,
                    "EventType": "STACK_EVENT",
                    "Timestamp": now,
                    "StartTime": started,
                    "EndTime": now,
                }),
            );
        });
        *self.operation.borrow_mut() = None;
    }

    /// The `DescribeEvents` view of a stack or resource event.
    fn mirror_event(
        &self,
        state: &mut CloudFormationState,
        logical_id: &str,
        physical_id: &str,
        resource_type: &str,
        status: &str,
        reason: Option<&str>,
    ) {
        let event_type = if status.ends_with("_FAILED") {
            "PROVISIONING_ERROR"
        } else {
            "PROGRESS"
        };
        let mut event = json!({
            "EventType": event_type,
            "LogicalResourceId": logical_id,
            "PhysicalResourceId": physical_id,
            "ResourceType": resource_type,
            "Timestamp": event_time(),
            "ResourceStatus": status,
        });
        if let Some(reason) = reason {
            event["ResourceStatusReason"] = json!(reason);
            if reason == VALIDATION_FAILED_SUMMARY {
                event["DetailedStatus"] = json!("VALIDATION_FAILED");
            }
        }
        self.file_operation_event(state, event);
    }

    fn with_state<T>(&self, f: impl FnOnce(&mut CloudFormationState) -> T) -> Option<T> {
        let mut accounts = self.state.write();
        accounts
            .regional_get_mut(self.account_id, self.region)
            .map(f)
    }

    fn with_stack<T>(&self, f: impl FnOnce(&mut Stack) -> T) -> Option<T> {
        self.with_state(|state| {
            state
                .stacks
                .values_mut()
                .find(|s| s.stack_id == self.stack_id && s.status != "DELETE_COMPLETE")
                .map(f)
        })
        .flatten()
    }

    fn read_stack(&self) -> Option<Stack> {
        let accounts = self.state.read();
        accounts
            .regional(self.account_id, self.region)?
            .stacks
            .values()
            .find(|s| s.stack_id == self.stack_id && s.status != "DELETE_COMPLETE")
            .cloned()
    }

    fn resource_event(
        &self,
        logical_id: &str,
        physical_id: &str,
        resource_type: &str,
        status: &str,
        reason: Option<&str>,
    ) {
        self.with_state(|state| {
            record_event_with_reason(
                state,
                &self.stack_id,
                &self.stack_name,
                logical_id,
                physical_id,
                resource_type,
                status,
                reason,
            );
            self.mirror_event(
                state,
                logical_id,
                physical_id,
                resource_type,
                status,
                reason,
            );
        });
    }

    /// Move the stack to `status` and record the event for it.
    fn transition(&self, status: &str, reason: Option<&str>) {
        self.with_state(|state| {
            if let Some(stack) = state
                .stacks
                .values_mut()
                .find(|s| s.stack_id == self.stack_id && s.status != "DELETE_COMPLETE")
            {
                stack.status = status.to_string();
            }
            record_event_with_reason(
                state,
                &self.stack_id,
                &self.stack_name,
                &self.stack_name,
                &self.stack_id,
                STACK_TYPE,
                status,
                reason,
            );
            let (name, id) = (self.stack_name.as_str(), self.stack_id.as_str());
            self.mirror_event(state, name, id, STACK_TYPE, status, reason);
        });
    }

    fn touch(&self, resource_type: &str, physical_id: &str) {
        let accounts = self.state.read();
        let mut touched = self.touched.borrow_mut();
        match accounts.regional(self.account_id, self.region) {
            Some(state) => {
                collect_nested_touched_types(state, resource_type, physical_id, &mut touched, 0)
            }
            None => touched.push(resource_type.to_string()),
        }
    }

    fn upsert_resource(&self, resource: &StackResource) {
        self.with_stack(|stack| {
            match stack
                .resources
                .iter_mut()
                .find(|r| r.logical_id == resource.logical_id)
            {
                Some(slot) => *slot = resource.clone(),
                None => stack.resources.push(resource.clone()),
            }
        });
    }

    fn remove_resource(&self, logical_id: &str) {
        self.with_stack(|stack| stack.resources.retain(|r| r.logical_id != logical_id));
    }

    fn set_resource_status(&self, logical_id: &str, status: &str) {
        self.with_stack(|stack| {
            if let Some(resource) = stack
                .resources
                .iter_mut()
                .find(|r| r.logical_id == logical_id)
            {
                resource.status = status.to_string();
            }
        });
    }

    fn outcome(&self, status: &str, succeeded: bool, reason: Option<String>) -> UpdateOutcome {
        UpdateOutcome {
            status: status.to_string(),
            succeeded,
            reason,
            touched_types: self.touched.take(),
        }
    }

    /// Claim the stack for an operation of `kind`, or say why it cannot start.
    /// One atomic step, so of two concurrent operations exactly one proceeds.
    pub(crate) fn begin(&self, kind: OperationKind) -> Result<(), String> {
        self.claim(kind, false)
    }

    /// `begin` for a nested stack. A child its held parent left waiting for the
    /// cleanup is claimable too: the parent's retry is what it was waiting for.
    fn begin_nested(&self) -> Result<(), String> {
        self.claim(OperationKind::Update, true)
    }

    fn claim(&self, kind: OperationKind, resuming_child: bool) -> Result<(), String> {
        let claimed = self.with_stack(|stack| {
            let allowed = match kind {
                OperationKind::Create => stack.status == "REVIEW_IN_PROGRESS",
                OperationKind::Update => {
                    status_allows_update(&stack.status)
                        || (resuming_child
                            && stack.status == "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"
                            && stack.pending_update.is_some())
                }
            };
            if !allowed {
                return Err(cannot_update_message(&stack.stack_id, &stack.status));
            }
            stack.status = kind.in_progress().to_string();
            stack.status_reason = None;
            stack.cancel_requested = false;
            stack.updated_at = Some(Utc::now());
            Ok(())
        });
        claimed.unwrap_or_else(|| Err(format!("Stack [{}] does not exist", self.stack_name)))
    }

    /// Apply the update to a stack `begin` has claimed.
    pub(crate) fn run(&self, input: UpdateInput<'_>) -> UpdateOutcome {
        self.open_operation(match input.kind {
            OperationKind::Create => "CREATE_STACK",
            OperationKind::Update => "UPDATE_STACK",
        });
        self.resource_event(
            &self.stack_name,
            &self.stack_id,
            STACK_TYPE,
            input.kind.in_progress(),
            Some(USER_INITIATED_REASON),
        );
        let Some(stack) = self.read_stack() else {
            return self.outcome(
                "DELETE_COMPLETE",
                false,
                Some(format!("Stack [{}] does not exist", self.stack_name)),
            );
        };
        let imported_names = self
            .with_state(|state| {
                state
                    .imports
                    .iter()
                    .filter(|(_, stacks)| stacks.contains(&self.stack_name))
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        // An earlier `DisableRollback` failure left a journal: this operation
        // continues it, so a rollback still reaches the last stable state.
        let continuing = stack.pending_update.is_some();
        let mut journal = stack
            .pending_update
            .clone()
            .unwrap_or_else(|| UpdateJournal {
                kind: input.kind,
                before: StackBaseline {
                    template: stack.template.clone(),
                    parameters: stack.parameters.clone(),
                    tags: stack.tags.clone(),
                    description: stack.description.clone(),
                    notification_arns: stack.notification_arns.clone(),
                    outputs: stack.outputs.clone(),
                    resources: stack.resources.clone(),
                    imported_names,
                },
                entries: Vec::new(),
                removed: Vec::new(),
                rollback_deletes: Vec::new(),
                restored: BTreeMap::new(),
                applied: BTreeMap::new(),
                released_entries: 0,
                retain_except_on_create: false,
            });
        let retried: BTreeSet<String> = journal
            .entries
            .iter()
            .filter_map(JournalEntry::retried_logical_id)
            .map(str::to_string)
            .collect();
        self.with_stack(|stack| stack.pending_update = None);

        // A continuing journal knows what each resource really holds; the stack's
        // template is only what the held attempt asked for.
        let previous: BTreeMap<String, ResourceDefinition> = if continuing {
            journal
                .applied
                .iter()
                .map(|(logical_id, saved)| {
                    (
                        logical_id.clone(),
                        saved.to_definition(logical_id, &saved.resource_type),
                    )
                })
                .collect()
        } else {
            let previous = self.resolve_previous(&stack, input.imports);
            journal.applied = previous
                .iter()
                .map(|(logical_id, def)| (logical_id.clone(), SavedDefinition::of(def)))
                .collect();
            previous
        };

        if let Some(conflict) = self.name_conflict(&stack, &input, &previous) {
            return self.validation_failed(journal, &input, &conflict);
        }
        if let Some(reason) = self
            .with_state(|state| {
                export_conflict(state, &self.stack_name, &self.stack_id, &input.export_names)
            })
            .flatten()
        {
            return self.stop(journal, &input, &reason, &reason);
        }
        if let Some(reason) = self.export_in_use_reason(&stack, &input) {
            return self.stop(journal, &input, &reason, &reason);
        }

        match self.forward(&stack, &input, &mut journal, &previous, &retried) {
            Ok(()) => self.commit(journal, &input),
            Err(failure) if failure.cancelled => self.roll_back(
                journal,
                RollbackOptions {
                    retain_except_on_create: input.retain_except_on_create,
                    nested: input.nested,
                    ..RollbackOptions::default()
                },
                Some(&failure.reason),
            ),
            Err(failure) => self.stop(journal, &input, &failure.reason, &failure.reason),
        }
    }

    /// `export_in_use` for an update about to start. An export value built from
    /// a resource this update changes is not known yet; the commit checks those.
    fn export_in_use_reason(&self, stack: &Stack, input: &UpdateInput<'_>) -> Option<String> {
        let outputs = CloudFormationService::resolve_template_outputs(
            input.template_body,
            input.parameters,
            &stack.resources,
            self.state,
            self.account_id,
            self.region,
        );
        let mut new_outputs: Vec<(String, Option<String>)> = input
            .export_names
            .iter()
            .map(|name| {
                let value = outputs
                    .iter()
                    .find(|o| o.export_name.as_ref() == Some(name))
                    .map(|o| o.value.clone());
                (name.clone(), value)
            })
            .collect();
        new_outputs.sort();
        self.with_state(|state| {
            if let Some(reason) = export_in_use(state, &self.stack_name, &new_outputs) {
                return Some(reason);
            }
            // Removing a nested stack removes its exports too.
            stack
                .resources
                .iter()
                .filter(|r| {
                    r.resource_type == STACK_TYPE
                        && !input
                            .parsed
                            .resources
                            .iter()
                            .any(|def| def.logical_id == r.logical_id)
                })
                .find_map(|r| {
                    let child = state
                        .stacks
                        .values()
                        .find(|s| s.stack_id == r.physical_id)?;
                    let (export, users) = family_export_in_use(state, child)?;
                    Some(format!(
                        "Delete canceled. Cannot delete export {export} as it is in use by {users}."
                    ))
                })
        })
        .flatten()
    }

    fn cancel_requested(&self) -> bool {
        self.read_stack()
            .is_some_and(|stack| stack.cancel_requested)
    }

    /// The resolved properties of each resource under the stored template, so
    /// an unchanged resource is left alone and a changed one can be classified
    /// and later re-applied. Empty when the old template no longer parses: every
    /// resource is then attempted, and there is nothing to re-apply.
    fn resolve_previous(
        &self,
        stack: &Stack,
        imports: &BTreeMap<String, String>,
    ) -> BTreeMap<String, ResourceDefinition> {
        let physical_ids: BTreeMap<String, String> = stack
            .resources
            .iter()
            .map(|r| (r.logical_id.clone(), r.physical_id.clone()))
            .collect();
        let attributes: BTreeMap<String, BTreeMap<String, String>> = stack
            .resources
            .iter()
            .map(|r| (r.logical_id.clone(), r.attributes.clone()))
            .collect();
        let Ok(parsed) = template::parse_template(&stack.template, &stack.parameters) else {
            return BTreeMap::new();
        };
        parsed
            .resources
            .iter()
            .filter_map(|def| {
                template::resolve_resource_properties_with_attrs(
                    def,
                    &stack.template,
                    &stack.parameters,
                    &physical_ids,
                    &attributes,
                    imports,
                )
                .ok()
                .map(|resolved| (def.logical_id.clone(), resolved))
            })
            .collect()
    }

    /// The E2 validation: a resource this update adds with a custom name another
    /// resource of the stack holds. Whether that other one is on its way out
    /// makes no difference, as it still exists when the new one is created.
    fn name_conflict(
        &self,
        stack: &Stack,
        input: &UpdateInput<'_>,
        previous: &BTreeMap<String, ResourceDefinition>,
    ) -> Option<ValidationConflict> {
        let physical_ids: BTreeMap<String, String> = stack
            .resources
            .iter()
            .map(|r| (r.logical_id.clone(), r.physical_id.clone()))
            .collect();
        let attributes: BTreeMap<String, BTreeMap<String, String>> = stack
            .resources
            .iter()
            .map(|r| (r.logical_id.clone(), r.attributes.clone()))
            .collect();
        for def in &input.parsed.resources {
            if stack
                .resources
                .iter()
                .any(|r| r.logical_id == def.logical_id)
            {
                continue;
            }
            // A name built from a resource created later is not known yet; the
            // create itself catches a collision on it.
            let Ok(resolved) = template::resolve_resource_properties_with_attrs(
                def,
                input.template_body,
                input.parameters,
                &physical_ids,
                &attributes,
                input.imports,
            ) else {
                continue;
            };
            let Some(name) = replacement::custom_name(&def.resource_type, &resolved.properties)
            else {
                continue;
            };
            let holder = previous.values().find(|held| {
                held.resource_type == def.resource_type
                    && replacement::custom_name(&held.resource_type, &held.properties).as_ref()
                        == Some(&name)
            });
            let display = replacement::display_name(&def.resource_type, &resolved.properties)
                .unwrap_or_else(|| name.clone());
            // Recorded with the holder removed, kept, or in another stack.
            let held_elsewhere = !replacement::name_is_scoped(&def.resource_type)
                && self.foreign_holder(&def.resource_type, &display).is_some();
            if holder.is_some() || held_elsewhere {
                return Some(ValidationConflict {
                    logical_id: def.logical_id.clone(),
                    resource_type: def.resource_type.clone(),
                    name: display,
                });
            }
        }
        None
    }

    fn validation_failed(
        &self,
        journal: UpdateJournal,
        input: &UpdateInput<'_>,
        conflict: &ValidationConflict,
    ) -> UpdateOutcome {
        let summary = VALIDATION_FAILED_SUMMARY;
        let issue = format!(
            "Resource of type '{}' with identifier '{}' already exists.",
            conflict.resource_type, conflict.name
        );
        let kind = self.operation.borrow().as_ref().map(|op| op.kind);
        self.with_state(|state| {
            self.file_operation_event(
                state,
                json!({
                    "OperationType": kind,
                    "EventType": "VALIDATION_ERROR",
                    "LogicalResourceId": conflict.logical_id,
                    "PhysicalResourceId": "",
                    "ResourceType": conflict.resource_type,
                    "Timestamp": event_time(),
                    "ValidationFailureMode": "FAIL",
                    "ValidationName": "NAME_CONFLICT_VALIDATION",
                    "ValidationStatus": "FAILED",
                    "ValidationStatusReason": issue,
                    "ValidationPath": format!("/Resources/{}", conflict.logical_id),
                }),
            );
        });
        self.resource_event(
            &self.stack_name,
            &self.stack_id,
            STACK_TYPE,
            input.kind.failed(),
            Some(summary),
        );
        self.stop(journal, input, VALIDATION_FAILURE_REASON, summary)
    }

    /// End a failed operation: roll back, or hold at `*_FAILED` when asked to.
    fn stop(
        &self,
        mut journal: UpdateJournal,
        input: &UpdateInput<'_>,
        rollback_reason: &str,
        failure_reason: &str,
    ) -> UpdateOutcome {
        if input.disable_rollback {
            let status = input.kind.failed();
            journal.released_entries = journal.entries.len();
            self.with_stack(|stack| {
                stack.status = status.to_string();
                stack.status_reason = Some(failure_reason.to_string());
                // Recorded: `GetTemplate` shows the attempt from here on. A retry
                // compares against the journal's applied definitions instead.
                stack.template = input.template_body.to_string();
                stack.parameters = input.parameters.clone();
                stack.pending_update = Some(journal);
            });
            self.resource_event(
                &self.stack_name,
                &self.stack_id,
                STACK_TYPE,
                status,
                Some(failure_reason),
            );
            self.close_operation("FAILED");
            return self.outcome(status, false, Some(failure_reason.to_string()));
        }
        // A failed create says one thing more, recorded for an export collision.
        let create_reason = format!(
            "{}. Rollback requested by user.",
            rollback_reason.trim_end().trim_end_matches('.')
        );
        self.roll_back(
            journal,
            RollbackOptions {
                retain_except_on_create: input.retain_except_on_create,
                nested: input.nested,
                ..RollbackOptions::default()
            },
            Some(match input.kind {
                OperationKind::Create => &create_reason,
                OperationKind::Update => rollback_reason,
            }),
        )
    }

    fn forward(
        &self,
        stack: &Stack,
        input: &UpdateInput<'_>,
        journal: &mut UpdateJournal,
        previous: &BTreeMap<String, ResourceDefinition>,
        retried: &BTreeSet<String>,
    ) -> Result<(), ForwardFailure> {
        let defs = &input.parsed.resources;
        let new_ids: BTreeSet<&str> = defs.iter().map(|d| d.logical_id.as_str()).collect();
        // A removed resource still exists, but the new template cannot name it.
        let mut physical_ids: BTreeMap<String, String> = stack
            .resources
            .iter()
            .filter(|r| new_ids.contains(r.logical_id.as_str()) && !r.physical_id.is_empty())
            .map(|r| (r.logical_id.clone(), r.physical_id.clone()))
            .collect();
        let mut attributes: BTreeMap<String, BTreeMap<String, String>> = stack
            .resources
            .iter()
            .filter(|r| new_ids.contains(r.logical_id.as_str()))
            .map(|r| (r.logical_id.clone(), r.attributes.clone()))
            .collect();
        // Custom names in use, removed resources included: they hold their name
        // until cleanup runs.
        let mut held: BTreeMap<(String, String), String> = previous
            .values()
            .filter_map(|def| {
                let name = replacement::custom_name(&def.resource_type, &def.properties)?;
                Some(((def.resource_type.clone(), name), def.logical_id.clone()))
            })
            .collect();

        let order = template::dependency_order(input.template_body, input.parameters, defs);
        for &index in &order {
            if self.cancel_requested() {
                return Err(ForwardFailure {
                    reason: USER_INITIATED_REASON.to_string(),
                    cancelled: true,
                });
            }
            let def = &defs[index];
            // A resource an earlier held update failed to create has no physical
            // resource to update: it is created again.
            let existing = stack
                .resources
                .iter()
                .find(|r| r.logical_id == def.logical_id && !r.physical_id.is_empty());
            let resolved = template::resolve_resource_properties_with_attrs(
                def,
                input.template_body,
                input.parameters,
                &physical_ids,
                &attributes,
                input.imports,
            )
            .map_err(|e| format!("Failed to resolve resource {}: {e}", def.logical_id));

            let applied = match existing {
                None => self.apply_create(def, resolved, journal, &mut held)?,
                Some(existing) => self.apply_existing(
                    existing,
                    def,
                    resolved,
                    previous.get(&def.logical_id),
                    journal,
                    &mut held,
                    retried.contains(&def.logical_id),
                    input,
                )?,
            };
            if let Some(resource) = applied {
                physical_ids.insert(resource.logical_id.clone(), resource.physical_id.clone());
                attributes.insert(resource.logical_id.clone(), resource.attributes.clone());
            }
        }
        Ok(())
    }

    fn fail_create(
        &self,
        logical_id: &str,
        resource_type: &str,
        journal: &mut UpdateJournal,
        reason: &str,
    ) -> ForwardFailure {
        self.resource_event(logical_id, "", resource_type, "CREATE_FAILED", Some(reason));
        journal.entries.push(JournalEntry::CreateFailed {
            logical_id: logical_id.to_string(),
            resource_type: resource_type.to_string(),
        });
        // Listed until the update resolves, as recorded after `DisableRollback`.
        self.upsert_resource(&StackResource {
            logical_id: logical_id.to_string(),
            physical_id: String::new(),
            resource_type: resource_type.to_string(),
            status: "CREATE_FAILED".to_string(),
            service_token: None,
            attributes: BTreeMap::new(),
            deletion_policy: None,
            update_replace_policy: None,
            custom_properties: None,
        });
        ForwardFailure {
            reason: format!("The following resource(s) failed to create: [{logical_id}]. "),
            cancelled: false,
        }
    }

    fn fail_update(
        &self,
        before: &StackResource,
        entry: JournalEntry,
        journal: &mut UpdateJournal,
        reason: &str,
    ) -> ForwardFailure {
        self.resource_event(
            &before.logical_id,
            &before.physical_id,
            &before.resource_type,
            "UPDATE_FAILED",
            Some(reason),
        );
        self.set_resource_status(&before.logical_id, "UPDATE_FAILED");
        journal.entries.push(entry);
        ForwardFailure {
            reason: format!(
                "The following resource(s) failed to update: [{}]. ",
                before.logical_id
            ),
            cancelled: false,
        }
    }

    /// A custom name another stack's resource holds.
    fn foreign_holder(&self, resource_type: &str, name: &str) -> Option<String> {
        let accounts = self.state.read();
        let state = accounts.regional(self.account_id, self.region)?;
        state
            .stacks
            .values()
            .filter(|s| s.stack_id != self.stack_id && s.status != "DELETE_COMPLETE")
            .find(|s| {
                s.resources.iter().any(|r| {
                    r.resource_type == resource_type && physical_id_names(&r.physical_id, name)
                })
            })
            .map(|s| s.stack_id.clone())
    }

    /// The reason a resource of `resource_type` cannot take the name its new
    /// properties ask for, when something in this region's stacks holds it.
    fn name_taken_reason(
        &self,
        logical_id: &str,
        resource_type: &str,
        properties: &Value,
        held: &BTreeMap<(String, String), String>,
    ) -> Option<String> {
        let name = replacement::custom_name(resource_type, properties)?;
        let display = replacement::display_name(resource_type, properties)?;
        let held_here = held
            .get(&(resource_type.to_string(), name))
            .is_some_and(|holder| holder != logical_id);
        if held_here {
            return Some(format!(
                "{display} already exists in stack {}",
                self.stack_id
            ));
        }
        // A scoped name is free elsewhere, and a physical id alone cannot say where.
        if replacement::name_is_scoped(resource_type) {
            return None;
        }
        self.foreign_holder(resource_type, &display)
            .map(|stack_id| format!("{display} already exists in stack {stack_id}"))
    }

    fn apply_create(
        &self,
        def: &ResourceDefinition,
        resolved: Result<ResourceDefinition, String>,
        journal: &mut UpdateJournal,
        held: &mut BTreeMap<(String, String), String>,
    ) -> Result<Option<StackResource>, ForwardFailure> {
        self.resource_event(
            &def.logical_id,
            "",
            &def.resource_type,
            "CREATE_IN_PROGRESS",
            None,
        );
        let resolved = resolved.map_err(|reason| {
            self.fail_create(&def.logical_id, &def.resource_type, journal, &reason)
        })?;
        if let Some(reason) = self.name_taken_reason(
            &def.logical_id,
            &def.resource_type,
            &resolved.properties,
            held,
        ) {
            return Err(self.fail_create(&def.logical_id, &def.resource_type, journal, &reason));
        }
        let mut resource = match guarded(|| self.provisioner.create_resource(&resolved)) {
            Ok(resource) => resource,
            Err(e) => {
                let reason = provisioning_reason(&e, false);
                let Some(physical_id) = self.provisioner.take_custom_failure(&def.logical_id)
                else {
                    return Err(self.fail_create(
                        &def.logical_id,
                        &def.resource_type,
                        journal,
                        &reason,
                    ));
                };
                // A handler that failed its create named a resource all the same,
                // and hears a Delete for it in the rollback, as recorded.
                self.resource_event(
                    &def.logical_id,
                    &physical_id,
                    &def.resource_type,
                    "CREATE_IN_PROGRESS",
                    Some(CREATE_INITIATED_REASON),
                );
                self.resource_event(
                    &def.logical_id,
                    &physical_id,
                    &def.resource_type,
                    "CREATE_FAILED",
                    Some(&reason),
                );
                let failed = StackResource {
                    logical_id: def.logical_id.clone(),
                    physical_id,
                    resource_type: def.resource_type.clone(),
                    status: "CREATE_FAILED".to_string(),
                    service_token: resolved
                        .properties
                        .get("ServiceToken")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    attributes: BTreeMap::new(),
                    deletion_policy: def.deletion_policy.clone(),
                    update_replace_policy: def.update_replace_policy.clone(),
                    custom_properties: None,
                };
                self.upsert_resource(&failed);
                journal
                    .entries
                    .push(JournalEntry::Created { resource: failed });
                return Err(ForwardFailure {
                    reason: format!(
                        "The following resource(s) failed to create: [{}]. ",
                        def.logical_id
                    ),
                    cancelled: false,
                });
            }
        };
        resource.attributes =
            merged_resource_attributes(self.provisioner, &resource, BTreeMap::new());
        if let Some(name) = replacement::custom_name(&def.resource_type, &resolved.properties) {
            held.insert((def.resource_type.clone(), name), def.logical_id.clone());
        }
        self.resource_event(
            &def.logical_id,
            &resource.physical_id,
            &def.resource_type,
            "CREATE_IN_PROGRESS",
            Some(CREATE_INITIATED_REASON),
        );
        self.upsert_resource(&resource);
        journal
            .applied
            .insert(def.logical_id.clone(), SavedDefinition::of(&resolved));
        journal.entries.push(JournalEntry::Created {
            resource: resource.clone(),
        });
        self.touch(&resource.resource_type, &resource.physical_id);
        self.resource_event(
            &def.logical_id,
            &resource.physical_id,
            &def.resource_type,
            "CREATE_COMPLETE",
            None,
        );
        Ok(Some(resource))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_existing(
        &self,
        existing: &StackResource,
        def: &ResourceDefinition,
        resolved: Result<ResourceDefinition, String>,
        previous: Option<&ResourceDefinition>,
        journal: &mut UpdateJournal,
        held: &mut BTreeMap<(String, String), String>,
        retried: bool,
        input: &UpdateInput<'_>,
    ) -> Result<Option<StackResource>, ForwardFailure> {
        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(reason) => {
                return Err(self.fail_update(
                    existing,
                    JournalEntry::UpdateFailed {
                        before: existing.clone(),
                        previous: None,
                    },
                    journal,
                    &reason,
                ))
            }
        };
        let saved = previous.map(SavedDefinition::of);
        let unchanged =
            !retried && previous.is_some_and(|old| old.properties == resolved.properties);
        if unchanged {
            // No action and no event, but the template's policies are metadata
            // the next delete reads.
            self.with_stack(|stack| {
                if let Some(slot) = stack
                    .resources
                    .iter_mut()
                    .find(|r| r.logical_id == def.logical_id)
                {
                    slot.deletion_policy = def.deletion_policy.clone();
                    slot.update_replace_policy = def.update_replace_policy.clone();
                }
            });
            return Ok(None);
        }

        let classification = match previous {
            Some(old) => {
                replacement::classify(&def.resource_type, &old.properties, &resolved.properties)
            }
            None => replacement::Classification::InPlace,
        };
        if classification.is_replacement() {
            return self.apply_replacement(existing, &resolved, previous, journal, held);
        }

        // Recorded with no in-progress event before it.
        if let Some(reason) = self.provisioner.update_refusal(&resolved) {
            return Err(self.fail_update(
                existing,
                JournalEntry::UpdateFailed {
                    before: existing.clone(),
                    previous: None,
                },
                journal,
                &reason,
            ));
        }

        self.resource_event(
            &existing.logical_id,
            &existing.physical_id,
            &existing.resource_type,
            "UPDATE_IN_PROGRESS",
            None,
        );
        self.touch(&existing.resource_type, &existing.physical_id);

        if existing.resource_type == STACK_TYPE {
            return self.apply_nested(existing, &resolved, journal, input);
        }

        let result = guarded(|| {
            if is_custom(&existing.resource_type) {
                self.provisioner
                    .update_custom_resource(
                        existing,
                        &resolved,
                        previous.map(|old| &old.properties),
                    )
                    .map(Some)
            } else {
                self.provisioner.update_resource(existing, &resolved)
            }
        });
        match result {
            Ok(Some(mut updated)) => {
                // Over the create-time attributes, so a second deploy's GetAtt
                // and Outputs do not collapse to the update handler's subset.
                updated.attributes = merged_resource_attributes(
                    self.provisioner,
                    &updated,
                    existing.attributes.clone(),
                );
                self.upsert_resource(&updated);
                journal
                    .applied
                    .insert(def.logical_id.clone(), SavedDefinition::of(&resolved));
                // A handler answering with another physical id made a new resource:
                // the old one is deleted in cleanup, like any replacement.
                journal.entries.push(
                    if is_custom(&existing.resource_type)
                        && updated.physical_id != existing.physical_id
                    {
                        JournalEntry::Replaced {
                            before: existing.clone(),
                            new: updated.clone(),
                        }
                    } else {
                        JournalEntry::Updated {
                            before: existing.clone(),
                            after: updated.clone(),
                            previous: saved,
                        }
                    },
                );
                self.touch(&updated.resource_type, &updated.physical_id);
                self.resource_event(
                    &updated.logical_id,
                    &updated.physical_id,
                    &updated.resource_type,
                    "UPDATE_COMPLETE",
                    None,
                );
                Ok(Some(updated))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                let lost = self.provisioner.take_reprovision_loss(&existing.logical_id);
                // A handler that said no may have acted on the request, and hears
                // an Update back to the old properties from what it was sent.
                let handler_answered = e.starts_with("Received response status");
                if handler_answered {
                    journal
                        .applied
                        .insert(def.logical_id.clone(), SavedDefinition::of(&resolved));
                }
                Err(self.fail_update(
                    existing,
                    JournalEntry::UpdateFailed {
                        before: existing.clone(),
                        previous: if lost || handler_answered {
                            saved
                        } else {
                            None
                        },
                    },
                    journal,
                    &provisioning_reason(&e, UPDATING),
                ))
            }
        }
    }

    fn apply_replacement(
        &self,
        existing: &StackResource,
        resolved: &ResourceDefinition,
        previous: Option<&ResourceDefinition>,
        journal: &mut UpdateJournal,
        held: &mut BTreeMap<(String, String), String>,
    ) -> Result<Option<StackResource>, ForwardFailure> {
        let refused = JournalEntry::UpdateFailed {
            before: existing.clone(),
            previous: None,
        };
        // E1: same event shape as an update that fails before it starts.
        if let Some(name) = previous
            .filter(|old| old.resource_type == resolved.resource_type)
            .and_then(|old| {
                replacement::custom_name_blocking_replacement(
                    &resolved.resource_type,
                    &old.properties,
                    &resolved.properties,
                )
            })
        {
            let handler_refusal = HANDLER_REFUSES_SAME_IDENTITY
                .iter()
                .find(|(listed, _)| *listed == resolved.resource_type);
            // The handler's turn: the replacement starts, and its create is what
            // fails, so there is a failed new resource to account for afterwards.
            if let Some((resource_type, message)) = handler_refusal {
                self.resource_event(
                    &existing.logical_id,
                    &existing.physical_id,
                    &existing.resource_type,
                    "UPDATE_IN_PROGRESS",
                    Some(REPLACEMENT_REASON),
                );
                let message = message.map(str::to_string).unwrap_or_else(|| {
                    format!(
                        "Resource of type '{resource_type}' with identifier '{name}' already exists."
                    )
                });
                return Err(self.fail_update(
                    existing,
                    JournalEntry::ReplaceFailed {
                        before: existing.clone(),
                    },
                    journal,
                    &handler_reason_with_code(&message, "AlreadyExists"),
                ));
            }
            self.resource_event(
                &existing.logical_id,
                &existing.physical_id,
                &existing.resource_type,
                "UPDATE_IN_PROGRESS",
                None,
            );
            let reason = format!(
                "CloudFormation cannot update a stack when a custom-named resource requires replacing. Rename {name} and update the stack again."
            );
            return Err(self.fail_update(existing, refused, journal, &reason));
        }
        // E6: the name is taken, and only the reasoned in-progress event was
        // recorded before the failure.
        if let Some(reason) = self.name_taken_reason(
            &resolved.logical_id,
            &resolved.resource_type,
            &resolved.properties,
            held,
        ) {
            self.resource_event(
                &existing.logical_id,
                &existing.physical_id,
                &existing.resource_type,
                "UPDATE_IN_PROGRESS",
                Some(REPLACEMENT_REASON),
            );
            return Err(self.fail_update(
                existing,
                JournalEntry::ReplaceFailed {
                    before: existing.clone(),
                },
                journal,
                &reason,
            ));
        }

        self.resource_event(
            &existing.logical_id,
            &existing.physical_id,
            &existing.resource_type,
            "UPDATE_IN_PROGRESS",
            None,
        );
        self.resource_event(
            &existing.logical_id,
            &existing.physical_id,
            &existing.resource_type,
            "UPDATE_IN_PROGRESS",
            Some(REPLACEMENT_REASON),
        );
        let created = guarded(|| self.provisioner.create_resource(resolved));
        let mut new = match created {
            Ok(new) => new,
            Err(e) => {
                return Err(self.fail_update(
                    existing,
                    JournalEntry::ReplaceFailed {
                        before: existing.clone(),
                    },
                    journal,
                    &provisioning_reason(&e, UPDATING),
                ))
            }
        };
        new.status = "UPDATE_COMPLETE".to_string();
        new.attributes = merged_resource_attributes(self.provisioner, &new, BTreeMap::new());
        if let Some(name) = replacement::custom_name(&resolved.resource_type, &resolved.properties)
        {
            held.insert(
                (resolved.resource_type.clone(), name),
                resolved.logical_id.clone(),
            );
        }
        self.resource_event(
            &new.logical_id,
            &new.physical_id,
            &new.resource_type,
            "UPDATE_IN_PROGRESS",
            Some(CREATE_INITIATED_REASON),
        );
        self.upsert_resource(&new);
        journal
            .applied
            .insert(resolved.logical_id.clone(), SavedDefinition::of(resolved));
        self.touch(&new.resource_type, &new.physical_id);
        self.touch(&existing.resource_type, &existing.physical_id);
        // An unmodeled type's physical id is its logical id, and some ids are
        // derived from properties that survived: the same id is the same
        // resource, so there is no old one to delete and the undo is a re-apply.
        journal
            .entries
            .push(if new.physical_id == existing.physical_id {
                JournalEntry::Updated {
                    before: existing.clone(),
                    after: new.clone(),
                    previous: previous.map(SavedDefinition::of),
                }
            } else {
                JournalEntry::Replaced {
                    before: existing.clone(),
                    new: new.clone(),
                }
            });
        self.resource_event(
            &new.logical_id,
            &new.physical_id,
            &new.resource_type,
            "UPDATE_COMPLETE",
            None,
        );
        Ok(Some(new))
    }

    fn nested_engine<'p>(
        &'p self,
        provisioner: &'p ResourceProvisioner,
        child_stack_id: &str,
        child_stack_name: &str,
    ) -> UpdateEngine<'p> {
        UpdateEngine::new(
            self.state,
            provisioner,
            self.account_id,
            self.region,
            child_stack_id,
            child_stack_name,
        )
    }

    /// A nested stack with no replacement is itself updated by this engine. The
    /// child's journal waits on the child stack for the parent's outcome.
    fn apply_nested(
        &self,
        existing: &StackResource,
        resolved: &ResourceDefinition,
        journal: &mut UpdateJournal,
        input: &UpdateInput<'_>,
    ) -> Result<Option<StackResource>, ForwardFailure> {
        let refused = |reason: String, journal: &mut UpdateJournal| {
            self.fail_update(
                existing,
                JournalEntry::UpdateFailed {
                    before: existing.clone(),
                    previous: None,
                },
                journal,
                &reason,
            )
        };
        let child = {
            let accounts = self.state.read();
            accounts
                .regional(self.account_id, self.region)
                .and_then(|s| {
                    s.stacks
                        .values()
                        .find(|st| {
                            st.stack_id == existing.physical_id && st.status != "DELETE_COMPLETE"
                        })
                        .map(|st| st.name.clone())
                })
        };
        let Some(child_name) = child else {
            return Err(refused(
                format!("Stack [{}] does not exist", existing.physical_id),
                journal,
            ));
        };
        let child_provisioner = self.provisioner.nested_provisioner(&existing.physical_id);
        let inputs = child_provisioner
            .nested_stack_inputs(resolved, &child_name)
            .map_err(|reason| refused(reason, journal))?;
        let parsed = template::parse_template(&inputs.template_body, &inputs.parameters)
            .map_err(|e| refused(format!("Failed to parse nested template: {e}"), journal))?;
        let engine = self.nested_engine(&child_provisioner, &existing.physical_id, &child_name);
        if let Some(reason) = engine
            .read_stack()
            .and_then(|child| type_change_refusal(&child, &parsed))
        {
            return Err(refused(reason, journal));
        }
        engine
            .begin_nested()
            .map_err(|reason| refused(reason, journal))?;
        let imports = CloudFormationService::collect_account_imports(
            self.state,
            self.account_id,
            self.region,
            Some(&child_name),
        );
        let outcome = engine.run(UpdateInput {
            kind: OperationKind::Update,
            template_body: &inputs.template_body,
            parsed: &parsed,
            parameters: &inputs.parameters,
            tags: inputs.tags,
            notification_arns: Vec::new(),
            imports: &imports,
            imported_names: template::imported_names(&inputs.template_body, &inputs.parameters),
            export_names: template::export_names(&inputs.template_body, &inputs.parameters),
            disable_rollback: input.disable_rollback,
            retain_except_on_create: input.retain_except_on_create,
            nested: true,
        });
        self.touched.borrow_mut().extend(outcome.touched_types);
        if !outcome.succeeded {
            let reason = format!(
                "Embedded stack {} was not successfully updated. Currently in {} with reason: {}",
                existing.physical_id,
                outcome.status,
                outcome.reason.unwrap_or_default()
            );
            let entry = failed_child_entry(existing, &outcome.status);
            return Err(self.fail_update(existing, entry, journal, &reason));
        }
        let mut updated = existing.clone();
        updated.status = "UPDATE_COMPLETE".to_string();
        updated.deletion_policy = resolved.deletion_policy.clone();
        updated.update_replace_policy = resolved.update_replace_policy.clone();
        if let Some(stack) = engine.read_stack() {
            for output in &stack.outputs {
                updated
                    .attributes
                    .insert(format!("Outputs.{}", output.key), output.value.clone());
            }
        }
        self.upsert_resource(&updated);
        journal
            .applied
            .insert(existing.logical_id.clone(), SavedDefinition::of(resolved));
        journal.entries.push(JournalEntry::NestedUpdated {
            before: existing.clone(),
            child_stack_id: existing.physical_id.clone(),
            failed: false,
        });
        self.resource_event(
            &updated.logical_id,
            &updated.physical_id,
            &updated.resource_type,
            "UPDATE_COMPLETE",
            None,
        );
        Ok(Some(updated))
    }

    /// The forward pass worked: make the new template the stack's, then clean up.
    fn commit(&self, mut journal: UpdateJournal, input: &UpdateInput<'_>) -> UpdateOutcome {
        let Some(stack) = self.read_stack() else {
            return self.outcome("DELETE_COMPLETE", false, None);
        };
        // Past the last resource boundary: a cancel still beats the commit.
        if stack.cancel_requested {
            return self.roll_back(
                journal,
                RollbackOptions {
                    retain_except_on_create: input.retain_except_on_create,
                    nested: input.nested,
                    ..RollbackOptions::default()
                },
                Some(USER_INITIATED_REASON),
            );
        }
        let new_ids: BTreeSet<&str> = input
            .parsed
            .resources
            .iter()
            .map(|d| d.logical_id.as_str())
            .collect();
        let (kept, removed): (Vec<StackResource>, Vec<StackResource>) = stack
            .resources
            .iter()
            .cloned()
            .partition(|r| new_ids.contains(r.logical_id.as_str()));
        let outputs = CloudFormationService::resolve_template_outputs(
            input.template_body,
            input.parameters,
            &kept,
            self.state,
            self.account_id,
            self.region,
        );
        journal.removed = removed;

        // Under one lock with the export registration, so a concurrent stack
        // that claimed one of these export names since the pre-flight check is
        // seen here. With the lock released during provisioning that can happen,
        // and registering over it would take the other stack's export.
        let mut tags = input.tags.clone();
        let mut notification_arns = input.notification_arns.clone();
        let conflict = self
            .with_state(|state| {
                let names: Vec<String> = outputs
                    .iter()
                    .filter_map(|o| o.export_name.clone())
                    .collect();
                if let Some(reason) =
                    export_conflict(state, &self.stack_name, &self.stack_id, &names)
                {
                    return Some(reason);
                }
                // The values are final now.
                let exported: Vec<(String, Option<String>)> = outputs
                    .iter()
                    .filter_map(|o| Some((o.export_name.clone()?, Some(o.value.clone()))))
                    .collect();
                if let Some(reason) = export_in_use(state, &self.stack_name, &exported) {
                    return Some(reason);
                }
                if let Some(stack) = state
                    .stacks
                    .values_mut()
                    .find(|s| s.stack_id == self.stack_id && s.status != "DELETE_COMPLETE")
                {
                    stack.template = input.template_body.to_string();
                    stack.parameters = input.parameters.clone();
                    stack.description = input.parsed.description.clone();
                    if !tags.is_empty() {
                        stack.tags = std::mem::take(&mut tags);
                    }
                    if !notification_arns.is_empty() {
                        stack.notification_arns = std::mem::take(&mut notification_arns);
                    }
                    stack.outputs = outputs.clone();
                }
                CloudFormationService::sync_exports_imports(
                    state,
                    &self.stack_id,
                    &self.stack_name,
                    &outputs,
                    &input.imported_names,
                );
                None
            })
            .flatten();
        if let Some(reason) = conflict {
            return self.stop(journal, input, &reason, &reason);
        }

        if input.nested {
            self.transition("UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", None);
            self.with_stack(|stack| stack.pending_update = Some(journal));
            return self.outcome("UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", true, None);
        }
        self.finish_success(journal)
    }

    /// Delete what the update left behind: removed resources and the old
    /// physical resources of replacements.
    fn store_journal(&self, journal: &UpdateJournal) {
        self.with_stack(|stack| stack.pending_update = Some(journal.clone()));
    }

    fn reach_cleanup_of(&self, _logical_id: &str) {
        #[cfg(test)]
        test_hooks::fire(&format!("cleanup:{_logical_id}"));
    }

    /// Finish a cleanup a restart interrupted, from the journal the stack kept.
    pub(crate) fn resume_cleanup(&self, journal: UpdateJournal, status: &str) -> UpdateOutcome {
        if status == "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS" {
            self.finish_rollback(journal)
        } else {
            self.finish_success(journal)
        }
    }

    fn finish_success(&self, mut journal: UpdateJournal) -> UpdateOutcome {
        let kind = journal.kind;
        if kind == OperationKind::Update {
            let in_cleanup = self
                .read_stack()
                .is_some_and(|s| s.status == "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS");
            if !in_cleanup {
                self.transition("UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", None);
            }
        }
        // On the stack, so a snapshot taken during the cleanup can finish it after
        // a restart. Trimmed as it goes: a delete repeated after a crash is harmless,
        // but one that already finished should not be redone.
        self.store_journal(&journal);
        for resource in deletion_order(&journal.before, &journal.removed) {
            self.reach_cleanup_of(&resource.logical_id);
            self.delete_for_cleanup(&resource, resource.deletion_policy.as_deref());
            self.remove_resource(&resource.logical_id);
            journal
                .removed
                .retain(|r| r.logical_id != resource.logical_id);
            self.store_journal(&journal);
        }
        while !journal.entries.is_empty() {
            match journal.entries.remove(0) {
                // The new template's policy decides, as recorded both ways.
                JournalEntry::Replaced { before, new } => {
                    self.reach_cleanup_of(&before.logical_id);
                    self.delete_for_cleanup(&before, new.update_replace_policy.as_deref());
                }
                JournalEntry::NestedUpdated { child_stack_id, .. } => {
                    let child = self.take_child(&child_stack_id);
                    if let Some((name, child_journal)) = child {
                        let provisioner = self.provisioner.nested_provisioner(&child_stack_id);
                        let engine = self.nested_engine(&provisioner, &child_stack_id, &name);
                        let outcome = engine.finish_success(child_journal);
                        self.touched.borrow_mut().extend(outcome.touched_types);
                    }
                }
                _ => {}
            }
            self.store_journal(&journal);
        }
        let complete = kind.complete();
        self.with_stack(|stack| {
            stack.status_reason = None;
            stack.pending_update = None;
            stack.updated_at = Some(Utc::now());
        });
        self.transition(complete, None);
        self.close_operation("SUCCEEDED");
        self.outcome(complete, true, None)
    }

    fn take_child(&self, child_stack_id: &str) -> Option<(String, UpdateJournal)> {
        self.with_state(|state| {
            let child = state
                .stacks
                .values_mut()
                .find(|s| s.stack_id == child_stack_id && s.status != "DELETE_COMPLETE")?;
            Some((child.name.clone(), child.pending_update.take()?))
        })
        .flatten()
    }

    /// Delete a resource in a cleanup phase. Whatever happens the stack carries
    /// on: a cleanup failure only leaves its event.
    fn delete_for_cleanup(&self, resource: &StackResource, policy: Option<&str>) {
        // Left from a held update that never created it.
        if resource.physical_id.is_empty() {
            self.resource_event(
                &resource.logical_id,
                "",
                &resource.resource_type,
                "DELETE_COMPLETE",
                None,
            );
            return;
        }
        self.touch(&resource.resource_type, &resource.physical_id);
        if policy_retains_physical(policy) {
            self.resource_event(
                &resource.logical_id,
                &resource.physical_id,
                &resource.resource_type,
                "DELETE_SKIPPED",
                None,
            );
            return;
        }
        self.delete_with_events(resource);
    }

    fn delete_with_events(&self, resource: &StackResource) {
        self.resource_event(
            &resource.logical_id,
            &resource.physical_id,
            &resource.resource_type,
            "DELETE_IN_PROGRESS",
            None,
        );
        match guarded(|| self.provisioner.delete_resource(resource)) {
            Ok(()) => self.resource_event(
                &resource.logical_id,
                &resource.physical_id,
                &resource.resource_type,
                "DELETE_COMPLETE",
                None,
            ),
            Err(e) => self.resource_event(
                &resource.logical_id,
                &resource.physical_id,
                &resource.resource_type,
                "DELETE_FAILED",
                Some(&provisioning_reason(&e, UPDATING)),
            ),
        }
    }

    /// Undo everything the journal recorded and restore the stack's baseline.
    /// `reason` goes on the rollback's first event; `None` means a caller asked
    /// for this rollback.
    pub(crate) fn roll_back(
        &self,
        mut journal: UpdateJournal,
        options: RollbackOptions,
        reason: Option<&str>,
    ) -> UpdateOutcome {
        let kind = journal.kind;
        self.close_operation("FAILED");
        self.open_operation(if options.continued {
            "CONTINUE_ROLLBACK"
        } else {
            "ROLLBACK"
        });
        self.with_stack(|stack| stack.pending_update = None);
        self.transition(
            kind.rollback_in_progress(),
            Some(reason.unwrap_or(USER_INITIATED_REASON)),
        );
        // Whatever is queued is this attempt's forward pass, for resources this
        // rollback undoes, and none of it has run. An earlier attempt's stop
        // already drained its own. The queues are shared with nested stacks, so
        // only the outermost rollback empties them.
        if !options.nested {
            self.provisioner.pending_container_spawns.lock().clear();
            self.provisioner.pending_custom_invokes.lock().clear();
        }

        while let Some(entry) = journal.entries.pop() {
            let released = journal.entries.len() < journal.released_entries;
            let mark = self.provisioner.pending_custom_invokes.lock().len();
            let undone = self.undo(&entry, &mut journal, &options, released);
            if !released {
                self.provisioner
                    .pending_custom_invokes
                    .lock()
                    .truncate(mark);
            }
            if let Err(logical_id) = undone {
                journal.entries.push(entry);
                return self.rollback_failed(journal, &logical_id);
            }
        }
        journal.released_entries = 0;
        journal.retain_except_on_create = options.retain_except_on_create;

        if kind == OperationKind::Update {
            self.transition("UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", None);
        }
        let mut outcome = self.finish_rollback(journal);
        outcome.reason = reason.map(str::to_string);
        outcome
    }

    /// A rollback's cleanup: delete what the update created, restore the baseline.
    fn finish_rollback(&self, mut journal: UpdateJournal) -> UpdateOutcome {
        let kind = journal.kind;
        let retain_except_on_create = journal.retain_except_on_create;
        // On the stack and trimmed as it goes, as in `finish_success`.
        self.store_journal(&journal);
        while !journal.rollback_deletes.is_empty() {
            let delete = journal.rollback_deletes.remove(0);
            match delete {
                RollbackDelete::Failed {
                    logical_id,
                    resource_type,
                } => {
                    self.resource_event(&logical_id, "", &resource_type, "DELETE_COMPLETE", None);
                }
                RollbackDelete::Created { resource, released } => {
                    self.reach_cleanup_of(&resource.logical_id);
                    let mark = self.provisioner.pending_custom_invokes.lock().len();
                    self.touch(&resource.resource_type, &resource.physical_id);
                    let retained = !retain_except_on_create
                        && is_plain_retain(resource.deletion_policy.as_deref());
                    if retained {
                        self.resource_event(
                            &resource.logical_id,
                            &resource.physical_id,
                            &resource.resource_type,
                            "DELETE_SKIPPED",
                            None,
                        );
                    } else {
                        self.delete_with_events(&resource);
                    }
                    if !released {
                        self.provisioner
                            .pending_custom_invokes
                            .lock()
                            .truncate(mark);
                    }
                }
                // Recorded: deleted even under `UpdateReplacePolicy: Retain`, which
                // is about the resource being replaced.
                RollbackDelete::Replacement { resource, released } => {
                    self.reach_cleanup_of(&resource.logical_id);
                    let mark = self.provisioner.pending_custom_invokes.lock().len();
                    self.touch(&resource.resource_type, &resource.physical_id);
                    self.delete_with_events(&resource);
                    if !released {
                        self.provisioner
                            .pending_custom_invokes
                            .lock()
                            .truncate(mark);
                    }
                }
            }
            self.store_journal(&journal);
        }

        let restored = std::mem::take(&mut journal.restored);
        let baseline = journal.before;
        self.with_state(|state| {
            if let Some(stack) = state
                .stacks
                .values_mut()
                .find(|s| s.stack_id == self.stack_id && s.status != "DELETE_COMPLETE")
            {
                stack.template = baseline.template.clone();
                stack.parameters = baseline.parameters.clone();
                stack.tags = baseline.tags.clone();
                stack.description = baseline.description.clone();
                stack.notification_arns = baseline.notification_arns.clone();
                stack.outputs = baseline.outputs.clone();
                stack.resources = baseline
                    .resources
                    .iter()
                    .map(|r| restored.get(&r.logical_id).unwrap_or(r).clone())
                    .collect();
                stack.status_reason = None;
                stack.pending_update = None;
                stack.cancel_requested = false;
                stack.updated_at = Some(Utc::now());
            }
            CloudFormationService::sync_exports_imports(
                state,
                &self.stack_id,
                &self.stack_name,
                &baseline.outputs,
                &baseline.imported_names,
            );
        });
        let status = kind.rollback_complete();
        self.transition(status, None);
        self.close_operation("SUCCEEDED");
        self.outcome(status, false, None)
    }

    fn undo(
        &self,
        entry: &JournalEntry,
        journal: &mut UpdateJournal,
        options: &RollbackOptions,
        released: bool,
    ) -> Result<(), String> {
        match entry {
            JournalEntry::Created { resource } => {
                journal.rollback_deletes.push(RollbackDelete::Created {
                    resource: resource.clone(),
                    released,
                });
            }
            JournalEntry::CreateFailed {
                logical_id,
                resource_type,
            } => {
                journal.rollback_deletes.push(RollbackDelete::Failed {
                    logical_id: logical_id.clone(),
                    resource_type: resource_type.clone(),
                });
            }
            JournalEntry::Updated {
                before,
                after,
                previous,
            } => {
                let current = after.clone();
                return self.reapply(before, &current, previous.as_ref(), journal, options);
            }
            JournalEntry::UpdateFailed { before, previous } => {
                return match previous {
                    Some(_) => self.reapply(before, before, previous.as_ref(), journal, options),
                    None => {
                        self.point_back(before, journal);
                        Ok(())
                    }
                };
            }
            JournalEntry::Replaced { before, new } => {
                self.point_back(before, journal);
                journal.rollback_deletes.push(RollbackDelete::Replacement {
                    resource: new.clone(),
                    released,
                });
            }
            JournalEntry::ReplaceFailed { before } => {
                self.point_back(before, journal);
                journal.rollback_deletes.push(RollbackDelete::Failed {
                    logical_id: before.logical_id.clone(),
                    resource_type: before.resource_type.clone(),
                });
            }
            JournalEntry::NestedUpdated {
                before,
                child_stack_id,
                ..
            } => {
                // Skipping the child leaves it as it is, journal and all.
                if options.skip.contains(&before.logical_id) {
                    return self.skip_reapply(before, journal);
                }
                self.resource_event(
                    &before.logical_id,
                    &before.physical_id,
                    &before.resource_type,
                    "UPDATE_IN_PROGRESS",
                    None,
                );
                if let Some((name, child_journal)) = self.take_child(child_stack_id) {
                    // `Child.Resource` names a resource inside the child.
                    let child_prefix = format!("{}.", before.logical_id);
                    let child_skip = options
                        .skip
                        .iter()
                        .filter_map(|id| id.strip_prefix(&child_prefix))
                        .map(str::to_string)
                        .collect();
                    let provisioner = self.provisioner.nested_provisioner(child_stack_id);
                    let engine = self.nested_engine(&provisioner, child_stack_id, &name);
                    let outcome = engine.roll_back(
                        child_journal,
                        RollbackOptions {
                            retain_except_on_create: options.retain_except_on_create,
                            nested: true,
                            force: options.force,
                            skip: child_skip,
                            continued: options.continued,
                        },
                        None,
                    );
                    self.touched.borrow_mut().extend(outcome.touched_types);
                    if outcome.status != OperationKind::Update.rollback_complete() {
                        return self.reapply_failed(
                            before,
                            &format!(
                                "Embedded stack {child_stack_id} was not successfully rolled back."
                            ),
                            journal,
                            options,
                        );
                    }
                }
                self.point_back(before, journal);
            }
        }
        Ok(())
    }

    /// Point a resource back at its old physical resource: the `UPDATE_COMPLETE`
    /// the recording shows with no in-progress event before it.
    fn point_back(&self, before: &StackResource, journal: &mut UpdateJournal) {
        let mut restored = before.clone();
        restored.status = "UPDATE_COMPLETE".to_string();
        self.upsert_resource(&restored);
        self.resource_event(
            &restored.logical_id,
            &restored.physical_id,
            &restored.resource_type,
            "UPDATE_COMPLETE",
            None,
        );
        journal
            .restored
            .insert(restored.logical_id.clone(), restored);
    }

    /// Give a resource its previous definition back.
    fn reapply(
        &self,
        before: &StackResource,
        current: &StackResource,
        previous: Option<&SavedDefinition>,
        journal: &mut UpdateJournal,
        options: &RollbackOptions,
    ) -> Result<(), String> {
        let Some(previous) = previous else {
            self.point_back(before, journal);
            return Ok(());
        };
        if options.skip.contains(&before.logical_id) {
            return self.skip_reapply(before, journal);
        }
        self.resource_event(
            &current.logical_id,
            &current.physical_id,
            &current.resource_type,
            "UPDATE_IN_PROGRESS",
            None,
        );
        #[cfg(test)]
        test_hooks::fire(&format!("undo:{}", before.logical_id));
        let definition = previous.to_definition(&before.logical_id, &before.resource_type);
        // What the handler holds now, which the undo is an update away from.
        let in_flight = journal
            .applied
            .get(&before.logical_id)
            .map(|saved| saved.properties.clone());
        let result = guarded(|| {
            if is_custom(&before.resource_type) {
                self.provisioner
                    .update_custom_resource(current, &definition, in_flight.as_ref())
                    .map(Some)
            } else {
                self.provisioner.update_resource(current, &definition)
            }
        });
        self.touch(&before.resource_type, &before.physical_id);
        self.provisioner.take_reprovision_loss(&before.logical_id);
        match result {
            Ok(result) => {
                let mut restored = before.clone();
                if let Some(updated) = result {
                    // Only a re-provision stand-in can move the physical id: the
                    // old one is gone, so the new one is the truth.
                    restored.physical_id = updated.physical_id.clone();
                    restored.attributes = merged_resource_attributes(
                        self.provisioner,
                        &updated,
                        before.attributes.clone(),
                    );
                }
                restored.status = "UPDATE_COMPLETE".to_string();
                self.upsert_resource(&restored);
                self.resource_event(
                    &restored.logical_id,
                    &restored.physical_id,
                    &restored.resource_type,
                    "UPDATE_COMPLETE",
                    None,
                );
                journal
                    .applied
                    .insert(before.logical_id.clone(), previous.clone());
                journal
                    .restored
                    .insert(restored.logical_id.clone(), restored);
                Ok(())
            }
            Err(e) => {
                self.reapply_failed(before, &provisioning_reason(&e, UPDATING), journal, options)
            }
        }
    }

    fn reapply_failed(
        &self,
        before: &StackResource,
        reason: &str,
        journal: &mut UpdateJournal,
        options: &RollbackOptions,
    ) -> Result<(), String> {
        self.resource_event(
            &before.logical_id,
            &before.physical_id,
            &before.resource_type,
            "UPDATE_FAILED",
            Some(reason),
        );
        self.set_resource_status(&before.logical_id, "UPDATE_FAILED");
        if options.force {
            return self.skip_reapply(before, journal);
        }
        Err(before.logical_id.clone())
    }

    /// `ResourcesToSkip`: the stack points at the old definition again, whatever
    /// the physical resource now holds.
    fn skip_reapply(
        &self,
        before: &StackResource,
        journal: &mut UpdateJournal,
    ) -> Result<(), String> {
        let mut restored = before.clone();
        restored.status = "UPDATE_COMPLETE".to_string();
        self.upsert_resource(&restored);
        self.resource_event(
            &restored.logical_id,
            &restored.physical_id,
            &restored.resource_type,
            "UPDATE_COMPLETE",
            Some("Resource skipped during ContinueUpdateRollback."),
        );
        journal
            .restored
            .insert(restored.logical_id.clone(), restored);
        Ok(())
    }

    fn rollback_failed(&self, journal: UpdateJournal, logical_id: &str) -> UpdateOutcome {
        let status = journal.kind.rollback_failed();
        let reason = format!("The following resource(s) failed to update: [{logical_id}]. ");
        self.with_stack(|stack| {
            stack.status_reason = Some(reason.clone());
            stack.pending_update = Some(journal);
        });
        self.transition(status, Some(&reason));
        self.close_operation("FAILED");
        self.outcome(status, false, Some(reason))
    }
}

/// The restart half of the `*_IN_PROGRESS` claim. A snapshot another operation
/// wrote mid-update can hold a stack in a status nothing will ever move it out
/// of, and which refuses every later operation. The update's undo log is gone with
/// the process, so a stack that had not reached its cleanup needs a person's look:
/// `UPDATE_ROLLBACK_FAILED`, where it can be deleted or continued. One in cleanup
/// had finished deciding, and takes the outcome it was on its way to.
pub(crate) fn settle_interrupted_update(stack: &mut Stack) {
    match stack.status.as_str() {
        // With a journal, `resume_interrupted_cleanups` finishes these. Without
        // one (a snapshot from before journals were kept here) there is nothing
        // left to do but report the outcome it was on its way to.
        "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS" if stack.pending_update.is_none() => {
            stack.status = "UPDATE_COMPLETE".to_string();
        }
        "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS" if stack.pending_update.is_none() => {
            stack.status = "UPDATE_ROLLBACK_COMPLETE".to_string();
        }
        "UPDATE_IN_PROGRESS" | "UPDATE_ROLLBACK_IN_PROGRESS" => {
            stack.status = "UPDATE_ROLLBACK_FAILED".to_string();
            stack.status_reason = Some("Interrupted by a server restart.".to_string());
        }
        _ => {}
    }
}

/// Whether a cleanup status has a journal to finish from.
fn resumable_cleanup(stack: &Stack) -> bool {
    stack.pending_update.is_some()
        && matches!(
            stack.status.as_str(),
            "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS" | "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS"
        )
}

/// The stacks whose journal another stack's journal still needs, followed down
/// through nested stacks: a parent's later cleanup or rollback finishes them.
fn stacks_waited_on(state: &CloudFormationState) -> BTreeSet<String> {
    let holds_on = |stack: &Stack| {
        resumable_cleanup(stack)
            || matches!(
                stack.status.as_str(),
                "UPDATE_FAILED" | "UPDATE_ROLLBACK_FAILED" | "CREATE_FAILED" | "ROLLBACK_FAILED"
            )
    };
    let mut waited_on: BTreeSet<String> = BTreeSet::new();
    loop {
        let before = waited_on.len();
        for stack in state.stacks.values() {
            if holds_on(stack) || waited_on.contains(&stack.stack_id) {
                if let Some(journal) = &stack.pending_update {
                    waited_on.extend(journal.nested_children().map(str::to_string));
                }
            }
        }
        if waited_on.len() == before {
            return waited_on;
        }
    }
}

/// The stacks to finish cleaning up after a restart, as `(id, name, status)`.
pub(crate) fn cleanups_to_resume(state: &CloudFormationState) -> Vec<(String, String, String)> {
    let waited_on = stacks_waited_on(state);
    state
        .stacks
        .values()
        .filter(|stack| resumable_cleanup(stack) && !waited_on.contains(&stack.stack_id))
        .map(|stack| {
            (
                stack.stack_id.clone(),
                stack.name.clone(),
                stack.status.clone(),
            )
        })
        .collect()
}

/// A stack's own name and id, and those of the stacks nested under it.
pub(crate) fn stack_family(state: &CloudFormationState, stack: &Stack) -> Vec<(String, String)> {
    let mut family = vec![(stack.name.clone(), stack.stack_id.clone())];
    let mut next = 0;
    while next < family.len() {
        let id = family[next].1.clone();
        next += 1;
        let Some(current) = state.stacks.values().find(|s| s.stack_id == id) else {
            continue;
        };
        for resource in &current.resources {
            if resource.resource_type != STACK_TYPE
                || family
                    .iter()
                    .any(|(_, known)| *known == resource.physical_id)
            {
                continue;
            }
            if let Some(child) = state
                .stacks
                .values()
                .find(|s| s.stack_id == resource.physical_id)
            {
                family.push((child.name.clone(), child.stack_id.clone()));
            }
        }
    }
    family
}

/// Whether `stack` or a stack nested under it holds a custom resource.
pub(crate) fn stack_family_has_custom_resource(state: &CloudFormationState, stack: &Stack) -> bool {
    stack_family(state, stack).iter().any(|(_, id)| {
        state
            .stacks
            .values()
            .find(|s| s.stack_id == *id)
            .is_some_and(|member| member.resources.iter().any(|r| is_custom(&r.resource_type)))
    })
}

/// An export `stack` or a stack nested under it owns that a stack outside that
/// family imports, as `(export, importers)`.
pub(crate) fn family_export_in_use(
    state: &CloudFormationState,
    stack: &Stack,
) -> Option<(String, String)> {
    let family = stack_family(state, stack);
    let in_family = |name: &str| family.iter().any(|(member, _)| member == name);
    state.exports.iter().find_map(|(name, export)| {
        if !family
            .iter()
            .any(|(_, id)| *id == export.exporting_stack_id)
        {
            return None;
        }
        let consumers: Vec<&str> = state
            .imports
            .get(name)?
            .iter()
            .map(String::as_str)
            .filter(|consumer| !in_family(consumer))
            .collect();
        (!consumers.is_empty()).then(|| (name.clone(), consumers.join(", ")))
    })
}

/// The parent's undo entry for a child that did not update. A child held at
/// `UPDATE_FAILED`, or stopped at `UPDATE_ROLLBACK_FAILED`, kept its journal for
/// the parent's rollback to reach; one that rolled back has nothing left to undo.
fn failed_child_entry(existing: &StackResource, child_status: &str) -> JournalEntry {
    if child_status == OperationKind::Update.failed()
        || child_status == OperationKind::Update.rollback_failed()
    {
        JournalEntry::NestedUpdated {
            before: existing.clone(),
            child_stack_id: existing.physical_id.clone(),
            failed: true,
        }
    } else {
        JournalEntry::UpdateFailed {
            before: existing.clone(),
            previous: None,
        }
    }
}

/// `settle_interrupted_update` for every stack of a region. A nested stack whose
/// parent still holds a journal keeps its own: the parent's later cleanup or
/// rollback needs it, and no status of the child's is final until then.
pub(crate) fn settle_interrupted_updates(state: &mut CloudFormationState) {
    let waited_on = stacks_waited_on(state);
    for stack in state.stacks.values_mut() {
        if !waited_on.contains(&stack.stack_id) {
            settle_interrupted_update(stack);
        }
    }
}

struct ValidationConflict {
    logical_id: String,
    resource_type: String,
    name: String,
}

/// `Retain` proper: `RetainExceptOnCreate` by definition does not keep what the
/// failed operation created, and `Snapshot` keeps a copy of a resource, which a
/// rollback of its creation has no use for.
pub(crate) fn is_plain_retain(policy: Option<&str>) -> bool {
    policy.is_some_and(|p| p.trim().eq_ignore_ascii_case("retain"))
}

/// `removed` ordered for deletion: the reverse of the old template's dependency
/// order, so a dependent goes before what it depends on.
fn deletion_order(baseline: &StackBaseline, removed: &[StackResource]) -> Vec<StackResource> {
    let Ok(parsed) = template::parse_template(&baseline.template, &baseline.parameters) else {
        return removed.iter().rev().cloned().collect();
    };
    let order =
        template::dependency_order(&baseline.template, &baseline.parameters, &parsed.resources);
    let rank: BTreeMap<&str, usize> = order
        .iter()
        .enumerate()
        .map(|(rank, &index)| (parsed.resources[index].logical_id.as_str(), rank))
        .collect();
    let mut sorted: Vec<StackResource> = removed.to_vec();
    sorted.sort_by_key(|r| {
        std::cmp::Reverse(
            rank.get(r.logical_id.as_str())
                .copied()
                .unwrap_or(usize::MAX),
        )
    });
    sorted
}

/// A change-set row for each resource the template adds or changes and each it
/// removes, computed the way an execution would decide. A resource that depends
/// on one being replaced will see a new value, so it is listed as well.
pub(crate) fn change_set_rows(
    stack: Option<&Stack>,
    parsed: &ParsedTemplate,
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    imports: &BTreeMap<String, String>,
) -> Vec<Value> {
    let mut physical_ids: BTreeMap<String, String> = BTreeMap::new();
    let mut attributes: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let resources: &[StackResource] = stack.map(|s| s.resources.as_slice()).unwrap_or(&[]);
    for r in resources {
        physical_ids.insert(r.logical_id.clone(), r.physical_id.clone());
        attributes.insert(r.logical_id.clone(), r.attributes.clone());
    }
    let held: Option<BTreeMap<String, Value>> =
        stack
            .and_then(|s| s.pending_update.as_ref())
            .map(|journal| {
                journal
                    .applied
                    .iter()
                    .map(|(id, saved)| (id.clone(), saved.properties.clone()))
                    .collect()
            });
    let previous: BTreeMap<String, Value> = held
        .or_else(|| {
            let s = stack?;
            let old = template::parse_template(&s.template, &s.parameters).ok()?;
            Some(
                old.resources
                    .iter()
                    .filter_map(|def| {
                        template::resolve_resource_properties_with_attrs(
                            def,
                            &s.template,
                            &s.parameters,
                            &physical_ids,
                            &attributes,
                            imports,
                        )
                        .ok()
                        .map(|resolved| (def.logical_id.clone(), resolved.properties))
                    })
                    .collect(),
            )
        })
        .unwrap_or_default();

    let defs = &parsed.resources;
    let mut rows: BTreeMap<String, Value> = BTreeMap::new();
    for &index in &template::dependency_order(template_body, parameters, defs) {
        let def = &defs[index];
        let Some(existing) = resources.iter().find(|r| r.logical_id == def.logical_id) else {
            rows.insert(
                def.logical_id.clone(),
                json!({
                    "Type": "Resource",
                    "ResourceChange": {
                        "Action": "Add",
                        "LogicalResourceId": def.logical_id,
                        "ResourceType": def.resource_type,
                    }
                }),
            );
            continue;
        };
        let resolved = template::resolve_resource_properties_with_attrs(
            def,
            template_body,
            parameters,
            &physical_ids,
            &attributes,
            imports,
        );
        let replacement_column = {
            match (previous.get(&def.logical_id), &resolved) {
                (Some(old), Ok(new)) if *old == new.properties => continue,
                (Some(old), Ok(new)) => {
                    match replacement::classify(&def.resource_type, old, &new.properties) {
                        replacement::Classification::InPlace => "False",
                        replacement::Classification::Replacement {
                            conditional: true, ..
                        } => "Conditional",
                        replacement::Classification::Replacement { .. } => "True",
                    }
                }
                _ => "Conditional",
            }
        };
        if replacement_column != "False" {
            // What refers to this resource sees a changed value.
            physical_ids.insert(
                def.logical_id.clone(),
                format!("{}-replacement", existing.physical_id),
            );
            if let Some(attrs) = attributes.get_mut(&def.logical_id) {
                for value in attrs.values_mut() {
                    value.push_str("-replacement");
                }
            }
        }
        rows.insert(
            def.logical_id.clone(),
            json!({
                "Type": "Resource",
                "ResourceChange": {
                    "Action": "Modify",
                    "LogicalResourceId": def.logical_id,
                    "PhysicalResourceId": existing.physical_id,
                    "ResourceType": def.resource_type,
                    "Replacement": replacement_column,
                }
            }),
        );
    }
    let mut out: Vec<Value> = defs
        .iter()
        .filter_map(|def| rows.remove(&def.logical_id))
        .collect();
    for r in resources {
        if !defs.iter().any(|d| d.logical_id == r.logical_id) {
            out.push(json!({
                "Type": "Resource",
                "ResourceChange": {
                    "Action": "Remove",
                    "LogicalResourceId": r.logical_id,
                    "PhysicalResourceId": r.physical_id,
                    "ResourceType": r.resource_type,
                }
            }));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_ids_name_a_resource_by_their_last_segment() {
        assert!(physical_id_names("t", "t"));
        assert!(physical_id_names("https://sqs/123/t", "t"));
        assert!(physical_id_names("arn:aws:sns:us-east-1:1:t", "t"));
        assert!(!physical_id_names("arn:aws:sns:us-east-1:1:not-t", "t"));
        assert!(!physical_id_names("other", "t"));
    }

    #[test]
    fn only_plain_retain_keeps_what_a_rollback_would_delete() {
        assert!(is_plain_retain(Some("Retain")));
        assert!(is_plain_retain(Some(" retain ")));
        assert!(!is_plain_retain(Some("RetainExceptOnCreate")));
        assert!(!is_plain_retain(Some("Snapshot")));
        assert!(!is_plain_retain(Some("Delete")));
        assert!(!is_plain_retain(None));
    }

    #[test]
    fn only_settled_states_accept_an_update() {
        for status in [
            "CREATE_COMPLETE",
            "UPDATE_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
            "UPDATE_FAILED",
        ] {
            assert!(status_allows_update(status), "{status}");
        }
        for status in [
            "UPDATE_IN_PROGRESS",
            "UPDATE_ROLLBACK_FAILED",
            "ROLLBACK_COMPLETE",
            "CREATE_FAILED",
            "REVIEW_IN_PROGRESS",
        ] {
            assert!(!status_allows_update(status), "{status}");
        }
    }

    #[test]
    fn export_owned_by_another_stack_is_the_conflict_reason() {
        let mut state = CloudFormationState::new("123456789012", "us-east-1");
        state.exports.insert(
            "shared".to_string(),
            crate::state::StackExport {
                value: "v".to_string(),
                exporting_stack_id: "arn:stack/a/1".to_string(),
                exporting_stack_name: "a".to_string(),
            },
        );
        assert_eq!(
            export_conflict(&state, "b", "arn:stack/b/2", &["shared".to_string()]).as_deref(),
            Some("Export with name shared is already exported by stack a")
        );
        assert_eq!(
            export_conflict(&state, "a", "arn:stack/a/1", &["shared".to_string()]),
            None
        );
        assert_eq!(
            export_conflict(&state, "b", "arn:stack/b/2", &["free".to_string()]),
            None
        );
    }

    mod through_the_service {
        use super::super::*;
        use crate::service::tests::{make_request, make_service};
        use std::collections::HashMap;

        const ACCOUNT: &str = "123456789012";
        const REGION: &str = "us-east-1";

        fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        }

        async fn call(svc: &CloudFormationService, action: &str, pairs: &[(&str, &str)]) {
            use fakecloud_core::service::AwsService;
            svc.handle(make_request(action, params(pairs)))
                .await
                .unwrap_or_else(|e| panic!("{action}: {}", e.message()));
        }

        async fn create(svc: &CloudFormationService, name: &str, template: &Value) {
            call(
                svc,
                "CreateStack",
                &[("StackName", name), ("TemplateBody", &template.to_string())],
            )
            .await;
        }

        async fn update(svc: &CloudFormationService, name: &str, template: &Value) {
            call(
                svc,
                "UpdateStack",
                &[("StackName", name), ("TemplateBody", &template.to_string())],
            )
            .await;
        }

        fn stack(svc: &CloudFormationService, name: &str) -> Stack {
            svc.state.read().regional(ACCOUNT, REGION).unwrap().stacks[name].clone()
        }

        fn statuses(svc: &CloudFormationService, name: &str, logical: &str) -> Vec<String> {
            let id = stack(svc, name).stack_id;
            svc.state.read().regional(ACCOUNT, REGION).unwrap().events[&id]
                .iter()
                .filter(|e| e["LogicalResourceId"] == logical)
                .map(|e| e["ResourceStatus"].as_str().unwrap().to_string())
                .collect()
        }

        /// Run the engine on a provisioner the test can look into afterwards.
        fn run_on(
            svc: &CloudFormationService,
            name: &str,
            template: &Value,
        ) -> (ResourceProvisioner, UpdateOutcome) {
            run_with(svc, name, template, |_| {})
        }

        fn run_held(
            svc: &CloudFormationService,
            name: &str,
            template: &Value,
        ) -> (ResourceProvisioner, UpdateOutcome) {
            run_full(svc, name, template, |_| {}, true)
        }

        /// `run_on`, doing something between the claim and the run, which is
        /// where another request would land with the lock released.
        fn run_with(
            svc: &CloudFormationService,
            name: &str,
            template: &Value,
            between: impl FnOnce(&CloudFormationService),
        ) -> (ResourceProvisioner, UpdateOutcome) {
            run_full(svc, name, template, between, false)
        }

        fn run_full(
            svc: &CloudFormationService,
            name: &str,
            template: &Value,
            between: impl FnOnce(&CloudFormationService),
            disable_rollback: bool,
        ) -> (ResourceProvisioner, UpdateOutcome) {
            let body = template.to_string();
            let stack_id = stack(svc, name).stack_id;
            let provisioner = svc.provisioner_deferred(&stack_id, ACCOUNT, REGION);
            let parameters = BTreeMap::new();
            let parsed = template::parse_template(&body, &parameters).unwrap();
            let imports = BTreeMap::new();
            let engine =
                UpdateEngine::new(&svc.state, &provisioner, ACCOUNT, REGION, &stack_id, name);
            engine.begin(OperationKind::Update).unwrap();
            between(svc);
            let outcome = engine.run(UpdateInput {
                kind: OperationKind::Update,
                template_body: &body,
                parsed: &parsed,
                parameters: &parameters,
                tags: BTreeMap::new(),
                notification_arns: Vec::new(),
                imports: &imports,
                imported_names: Vec::new(),
                // Empty here, so the pre-flight sees nothing and only the commit
                // can catch what `between` did.
                export_names: Vec::new(),
                disable_rollback,
                retain_except_on_create: false,
                nested: false,
            });
            (provisioner, outcome)
        }

        fn bad_queue() -> Value {
            json!({
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "not a valid name!"}
            })
        }

        fn custom(value: &str) -> Value {
            json!({
                "Type": "Custom::Thing",
                "Properties": {
                    "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:f",
                    "Value": value
                }
            })
        }

        /// The handler has seen nothing of an update that rolled back, so it must
        /// hear nothing, whether of the create, the update or the undo.
        #[tokio::test]
        async fn a_rollback_fires_no_deferred_custom_resource_invokes() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let (provisioner, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {
                    "Anchor": {"Type": "AWS::SNS::Topic"},
                    "Thing": custom("a"),
                    "Bad": {
                        "Type": "AWS::SQS::Queue",
                        "DependsOn": "Thing",
                        "Properties": {"QueueName": "not a valid name!"}
                    }
                }}),
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            assert!(provisioner.pending_custom_invokes.lock().is_empty());
        }

        #[tokio::test]
        async fn a_custom_resource_update_keeps_its_id_and_sends_update() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let (provisioner, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}, "Thing": custom("a")}}),
            );
            assert!(outcome.succeeded);
            assert_eq!(provisioner.pending_custom_invokes.lock().len(), 1);
            let physical = stack(&svc, "s")
                .resources
                .iter()
                .find(|r| r.logical_id == "Thing")
                .unwrap()
                .physical_id
                .clone();

            let (provisioner, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}, "Thing": custom("b")}}),
            );
            assert!(outcome.succeeded);
            let invokes = provisioner.pending_custom_invokes.lock();
            assert_eq!(invokes.len(), 1);
            let event: Value = serde_json::from_str(&invokes[0].payload).unwrap();
            assert_eq!(event["RequestType"], "Update");
            assert_eq!(event["PhysicalResourceId"], physical.as_str());
            assert_eq!(event["ResourceProperties"]["Value"], "b");
            assert_eq!(event["OldResourceProperties"]["Value"], "a");
            drop(invokes);
            let resources = stack(&svc, "s").resources;
            assert_eq!(
                resources
                    .iter()
                    .find(|r| r.logical_id == "Thing")
                    .unwrap()
                    .physical_id,
                physical
            );
        }

        #[tokio::test]
        async fn a_removed_custom_resource_is_told_to_delete_only_on_success() {
            let svc = make_service();
            let with_thing = json!({"Resources": {
                "Anchor": {"Type": "AWS::SNS::Topic"},
                "Thing": custom("a")
            }});
            // CreateStack invokes synchronously, so go through the engine.
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            run_on(&svc, "s", &with_thing);

            let (provisioner, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}, "Bad": bad_queue()}}),
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            assert!(provisioner.pending_custom_invokes.lock().is_empty());

            let (provisioner, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            );
            assert!(outcome.succeeded);
            let invokes = provisioner.pending_custom_invokes.lock();
            assert_eq!(invokes.len(), 1);
            assert!(invokes[0].payload.contains("\"RequestType\":\"Delete\""));
        }

        fn instance(image: &str) -> Value {
            json!({"Type": "AWS::EC2::Instance", "Properties": {"ImageId": image}})
        }

        #[tokio::test]
        async fn containers_spawn_only_for_what_survives_and_tear_down_what_goes() {
            let svc = make_service();
            create(&svc, "s", &json!({"Resources": {"Box": instance("ami-1")}})).await;
            let old = stack(&svc, "s").resources[0].physical_id.clone();

            // Rolled back: the replacement's container never starts, and the only
            // teardown is the replacement's own.
            let (provisioner, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {
                    "Box": instance("ami-2"),
                    "Bad": {
                        "Type": "AWS::SQS::Queue",
                        "DependsOn": "Box",
                        "Properties": {"QueueName": "not a valid name!"}
                    }
                }}),
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            assert!(provisioner.pending_container_spawns.lock().is_empty());
            {
                let teardowns = provisioner.pending_container_teardowns.lock();
                assert_eq!(teardowns.len(), 1);
                assert!(!format!("{:?}", teardowns[0]).contains(&old));
            }
            assert_eq!(stack(&svc, "s").resources[0].physical_id, old);

            // Applied: the new container starts, the old one goes.
            let (provisioner, outcome) =
                run_on(&svc, "s", &json!({"Resources": {"Box": instance("ami-2")}}));
            assert!(outcome.succeeded);
            assert_eq!(provisioner.pending_container_spawns.lock().len(), 1);
            let teardowns = provisioner.pending_container_teardowns.lock();
            assert_eq!(teardowns.len(), 1);
            assert!(format!("{:?}", teardowns[0]).contains(&old));
            assert_ne!(stack(&svc, "s").resources[0].physical_id, old);
        }

        fn stream(name: Option<&str>, prefix: &str, source: Option<&str>) -> Value {
            let mut props = json!({
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "Prefix": prefix
                }
            });
            if let Some(name) = name {
                props["DeliveryStreamName"] = json!(name);
            }
            if let Some(source) = source {
                props["DeliveryStreamType"] = json!("KinesisStreamAsSource");
                props["KinesisStreamSourceConfiguration"] = json!({
                    "KinesisStreamARN": source,
                    "RoleARN": "arn:aws:iam::123456789012:role/fh"
                });
            }
            json!({"Type": "AWS::KinesisFirehose::DeliveryStream", "Properties": props})
        }

        fn stream_names(svc: &CloudFormationService) -> Vec<String> {
            let firehose = svc.deps.firehose.read();
            firehose
                .get(ACCOUNT)
                .and_then(|a| a.streams(REGION))
                .map(|s| s.keys().cloned().collect())
                .unwrap_or_default()
        }

        #[tokio::test]
        async fn firehose_rename_creates_the_new_stream_then_deletes_the_old() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "a/", None)}}),
            )
            .await;
            update(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("two"), "a/", None)}}),
            )
            .await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_COMPLETE");
            assert_eq!(stream_names(&svc), ["two"]);
            assert_eq!(stack(&svc, "s").resources[0].physical_id, "two");
        }

        #[tokio::test]
        async fn firehose_source_change_on_an_unnamed_stream_gets_a_new_name() {
            let svc = make_service();
            let source =
                |name: &str| format!("arn:aws:kinesis:us-east-1:123456789012:stream/{name}");
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(None, "a/", Some(&source("one")))}}),
            )
            .await;
            let old = stack(&svc, "s").resources[0].physical_id.clone();
            update(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(None, "a/", Some(&source("two")))}}),
            )
            .await;
            let new = stack(&svc, "s").resources[0].physical_id.clone();
            assert_ne!(new, old);
            assert_eq!(stream_names(&svc), [new]);
        }

        #[tokio::test]
        async fn firehose_destination_change_updates_in_place() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "a/", None)}}),
            )
            .await;
            let (instance, version) = {
                let firehose = svc.deps.firehose.read();
                let stored = &firehose.get(ACCOUNT).unwrap().streams(REGION).unwrap()["one"];
                (stored.instance_id, stored.version_id.clone())
            };
            update(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "b/", None)}}),
            )
            .await;
            let firehose = svc.deps.firehose.read();
            let stored = &firehose.get(ACCOUNT).unwrap().streams(REGION).unwrap()["one"];
            assert_eq!(stored.instance_id, instance);
            assert_ne!(stored.version_id, version);
        }

        #[tokio::test]
        async fn firehose_replacement_with_a_bad_template_keeps_the_live_stream() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "a/", None)}}),
            )
            .await;
            let mut bad = stream(Some("two"), "a/", None);
            bad["Properties"]["ExtendedS3DestinationConfiguration"]["CompressionFormat"] =
                json!("BROTLI");
            update(&svc, "s", &json!({"Resources": {"F": bad}})).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(stream_names(&svc), ["one"]);
        }

        #[tokio::test]
        async fn firehose_rename_into_another_stacks_stream_is_refused() {
            let svc = make_service();
            create(
                &svc,
                "a",
                &json!({"Resources": {"F": stream(Some("taken"), "a/", None)}}),
            )
            .await;
            create(
                &svc,
                "b",
                &json!({"Resources": {"F": stream(Some("mine"), "a/", None)}}),
            )
            .await;
            let holder = stack(&svc, "a").stack_id;
            update(
                &svc,
                "b",
                &json!({"Resources": {"F": stream(Some("taken"), "a/", None)}}),
            )
            .await;
            assert_eq!(stack(&svc, "b").status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(stream_names(&svc), ["mine", "taken"]);
            let events = svc.state.read().regional(ACCOUNT, REGION).unwrap().events
                [&stack(&svc, "b").stack_id]
                .clone();
            assert!(events.iter().any(|e| {
                e["ResourceStatus"] == "UPDATE_FAILED"
                    && e["ResourceStatusReason"]
                        == format!("taken already exists in stack {holder}").as_str()
            }));
        }

        /// `UpdateReplacePolicy` decides the old resource's fate in the cleanup.
        #[tokio::test]
        async fn update_replace_policy_retain_skips_the_old_resources_delete() {
            let svc = make_service();
            let with_policy = |name: &str| {
                let mut def = stream(Some(name), "a/", None);
                def["UpdateReplacePolicy"] = json!("Retain");
                json!({"Resources": {"F": def}})
            };
            create(&svc, "s", &with_policy("one")).await;
            update(&svc, "s", &with_policy("two")).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_COMPLETE");
            assert_eq!(stream_names(&svc), ["one", "two"]);
            assert!(statuses(&svc, "s", "F").contains(&"DELETE_SKIPPED".to_string()));
        }

        /// `DeletionPolicy` decides a removed resource's fate in the cleanup.
        #[tokio::test]
        async fn deletion_policy_retain_skips_a_removed_resources_delete() {
            let svc = make_service();
            let mut def = stream(Some("one"), "a/", None);
            def["DeletionPolicy"] = json!("Retain");
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": def, "Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            update(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            assert_eq!(stream_names(&svc), ["one"]);
            assert!(statuses(&svc, "s", "F").contains(&"DELETE_SKIPPED".to_string()));
            assert_eq!(stack(&svc, "s").resources.len(), 1);
        }

        fn added_then_bad(policy: Option<&str>) -> Value {
            let mut added = stream(Some("added"), "a/", None);
            if let Some(policy) = policy {
                added["DeletionPolicy"] = json!(policy);
            }
            json!({"Resources": {
                "Anchor": {"Type": "AWS::SNS::Topic"},
                "F": added,
                "Bad": {
                    "Type": "AWS::SQS::Queue",
                    "DependsOn": "F",
                    "Properties": {"QueueName": "not a valid name!"}
                }
            }})
        }

        /// `RetainExceptOnCreate`, the parameter and the policy, delete what the
        /// failed operation created; plain `Retain` keeps it, as the model says.
        #[tokio::test]
        async fn retain_rules_for_what_a_rollback_would_delete() {
            for (policy, flag, kept) in [
                (Some("Retain"), false, true),
                (Some("Retain"), true, false),
                (Some("RetainExceptOnCreate"), false, false),
                (None, false, false),
            ] {
                let svc = make_service();
                create(
                    &svc,
                    "s",
                    &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
                )
                .await;
                let body = added_then_bad(policy).to_string();
                let mut args = vec![("StackName", "s"), ("TemplateBody", body.as_str())];
                if flag {
                    args.push(("RetainExceptOnCreate", "true"));
                }
                call(&svc, "UpdateStack", &args).await;
                assert_eq!(stack(&svc, "s").status, "UPDATE_ROLLBACK_COMPLETE");
                assert_eq!(
                    stream_names(&svc).contains(&"added".to_string()),
                    kept,
                    "{policy:?} {flag}"
                );
            }
        }

        fn held_update(prefix: &str) -> Value {
            json!({"Resources": {
                "F": stream(Some("one"), prefix, None),
                "Bad": {
                    "Type": "AWS::SQS::Queue",
                    "DependsOn": "F",
                    "Properties": {"QueueName": "not a valid name!"}
                }
            }})
        }

        fn remove_stream(svc: &CloudFormationService, name: &str) {
            let mut firehose = svc.deps.firehose.write();
            firehose
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .remove(name);
        }

        async fn held_stack(svc: &CloudFormationService) {
            create(
                svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "a/", None)}}),
            )
            .await;
            call(
                svc,
                "UpdateStack",
                &[
                    ("StackName", "s"),
                    ("TemplateBody", &held_update("b/").to_string()),
                    ("DisableRollback", "true"),
                ],
            )
            .await;
            assert_eq!(stack(svc, "s").status, "UPDATE_FAILED");
        }

        #[tokio::test]
        async fn a_held_update_can_be_retried_to_success() {
            let svc = make_service();
            held_stack(&svc).await;
            // The failed resource is attempted again; the fixed template applies.
            update(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "b/", None)}}),
            )
            .await;
            let after = stack(&svc, "s");
            assert_eq!(after.status, "UPDATE_COMPLETE");
            assert!(after.pending_update.is_none());
            // Failing again from UPDATE_FAILED, this time rolling back, reaches the
            // state before the first failed update's journal only when one is
            // still held; here the journal is gone, so it reaches this template.
            update(&svc, "s", &held_update("c/")).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_ROLLBACK_COMPLETE");
            let firehose = svc.deps.firehose.read();
            let stored = &firehose.get(ACCOUNT).unwrap().streams(REGION).unwrap()["one"];
            assert_eq!(
                stored.destination.as_ref().unwrap().prefix.as_deref(),
                Some("b/")
            );
        }

        #[tokio::test]
        async fn a_rollback_that_cannot_reapply_stops_at_rollback_failed() {
            let svc = make_service();
            held_stack(&svc).await;
            remove_stream(&svc, "one");

            call(&svc, "RollbackStack", &[("StackName", "s")]).await;
            let failed = stack(&svc, "s");
            assert_eq!(failed.status, "UPDATE_ROLLBACK_FAILED");
            assert_eq!(
                failed.status_reason.as_deref(),
                Some("The following resource(s) failed to update: [F]. ")
            );
            assert!(failed.pending_update.is_some(), "kept for the next attempt");

            // It accepts no update, only a continuation.
            use fakecloud_core::service::AwsService;
            let body = json!({"Resources": {"F": stream(Some("one"), "z/", None)}}).to_string();
            let refused = svc
                .handle(make_request(
                    "UpdateStack",
                    params(&[("StackName", "s"), ("TemplateBody", &body)]),
                ))
                .await
                .err()
                .expect("refused");
            assert!(refused.message().contains("UPDATE_ROLLBACK_FAILED"));

            // Still broken: fails again.
            call(&svc, "ContinueUpdateRollback", &[("StackName", "s")]).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_ROLLBACK_FAILED");

            // Skipping it completes the rollback.
            call(
                &svc,
                "ContinueUpdateRollback",
                &[("StackName", "s"), ("ResourcesToSkip.member.1", "F")],
            )
            .await;
            let done = stack(&svc, "s");
            assert_eq!(done.status, "UPDATE_ROLLBACK_COMPLETE");
            assert!(done.pending_update.is_none());
            assert_eq!(done.status_reason, None);
        }

        #[tokio::test]
        async fn force_rollback_skips_what_cannot_reapply() {
            let svc = make_service();
            held_stack(&svc).await;
            remove_stream(&svc, "one");
            call(&svc, "RollbackStack", &[("StackName", "s")]).await;
            call(
                &svc,
                "ContinueUpdateRollback",
                &[("StackName", "s"), ("ForceRollback", "true")],
            )
            .await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_ROLLBACK_COMPLETE");
        }

        #[tokio::test]
        async fn continue_rollback_refuses_a_stack_with_nothing_to_continue() {
            use fakecloud_core::service::AwsService;
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "a/", None)}}),
            )
            .await;
            let err = svc
                .handle(make_request(
                    "ContinueUpdateRollback",
                    params(&[("StackName", "s")]),
                ))
                .await
                .err()
                .expect("refused");
            assert_eq!(err.code(), "ValidationError");
            assert_eq!(
                err.message(),
                "RollbackUpdatedStack cannot be called from current stack status"
            );
        }

        #[tokio::test]
        async fn an_in_progress_update_blocks_other_operations() {
            use fakecloud_core::service::AwsService;
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            {
                let mut accounts = svc.state.write();
                accounts
                    .regional_get_mut(ACCOUNT, REGION)
                    .unwrap()
                    .stacks
                    .get_mut("s")
                    .unwrap()
                    .status = "UPDATE_IN_PROGRESS".to_string();
            }
            let body = json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}).to_string();
            for (action, pairs) in [
                (
                    "UpdateStack",
                    vec![("StackName", "s"), ("TemplateBody", body.as_str())],
                ),
                ("DeleteStack", vec![("StackName", "s")]),
            ] {
                let err = svc
                    .handle(make_request(action, params(&pairs)))
                    .await
                    .err()
                    .unwrap_or_else(|| panic!("{action} must be refused"));
                assert!(err.message().contains("UPDATE_IN_PROGRESS"), "{action}");
            }
        }

        /// Cloud Control has no replacement: a create-only patch is refused and
        /// changes nothing, anything else goes to the update handler.
        #[tokio::test]
        async fn cloud_control_refuses_a_create_only_patch() {
            let svc = make_service();
            let created = svc
                .cloudcontrol_create(
                    "AWS::SQS::Queue",
                    json!({"QueueName": "cc-queue"}),
                    ACCOUNT,
                    REGION,
                )
                .unwrap();
            let err = svc
                .cloudcontrol_update(
                    "AWS::SQS::Queue",
                    &created.physical_id,
                    &created.attributes,
                    &json!({"QueueName": "cc-queue"}),
                    json!({"QueueName": "cc-other"}),
                    ACCOUNT,
                    REGION,
                )
                .expect_err("refused");
            assert_eq!(
                err,
                "Invalid patch update: createOnlyProperties [/properties/QueueName] cannot be updated"
            );
            let updated = svc
                .cloudcontrol_update(
                    "AWS::SQS::Queue",
                    &created.physical_id,
                    &created.attributes,
                    &json!({"QueueName": "cc-queue"}),
                    json!({"QueueName": "cc-queue", "VisibilityTimeout": 60}),
                    ACCOUNT,
                    REGION,
                )
                .unwrap();
            assert_eq!(updated.physical_id, created.physical_id);
        }

        #[tokio::test]
        async fn a_stack_interrupted_by_a_restart_is_not_stuck() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "a/", None)}}),
            )
            .await;
            let set_status = |status: &str| {
                let mut accounts = svc.state.write();
                accounts
                    .regional_get_mut(ACCOUNT, REGION)
                    .unwrap()
                    .stacks
                    .get_mut("s")
                    .unwrap()
                    .status = status.to_string();
            };
            for (interrupted, settled) in [
                // With no journal to finish from.
                ("UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", "UPDATE_COMPLETE"),
                (
                    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                    "UPDATE_ROLLBACK_COMPLETE",
                ),
                ("UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED"),
                ("UPDATE_ROLLBACK_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED"),
                ("CREATE_COMPLETE", "CREATE_COMPLETE"),
            ] {
                let mut held = stack(&svc, "s");
                held.status = interrupted.to_string();
                settle_interrupted_update(&mut held);
                assert_eq!(held.status, settled);
            }
            // With its undo log gone, a continuation just settles the stack.
            set_status("UPDATE_ROLLBACK_FAILED");
            call(&svc, "ContinueUpdateRollback", &[("StackName", "s")]).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_ROLLBACK_COMPLETE");
        }

        fn prefix_of(svc: &CloudFormationService, name: &str) -> String {
            let firehose = svc.deps.firehose.read();
            firehose.get(ACCOUNT).unwrap().streams(REGION).unwrap()[name]
                .destination
                .as_ref()
                .unwrap()
                .prefix
                .clone()
                .unwrap()
        }

        fn failing_queue(depends_on: &str) -> Value {
            json!({
                "Type": "AWS::SQS::Queue",
                "DependsOn": depends_on,
                "Properties": {"QueueName": "not a valid name!"}
            })
        }

        async fn update_held(svc: &CloudFormationService, name: &str, template: &Value) {
            call(
                svc,
                "UpdateStack",
                &[
                    ("StackName", name),
                    ("TemplateBody", &template.to_string()),
                    ("DisableRollback", "true"),
                ],
            )
            .await;
        }

        /// Review finding: a retry diffed against the attempted template, so what
        /// the failed attempt never reached was skipped as unchanged.
        #[tokio::test]
        async fn a_retry_applies_what_the_held_attempt_never_reached() {
            let svc = make_service();
            let template = |prefix: &str, bad: bool| {
                let mut after = stream(Some("after"), prefix, None);
                let mut resources = json!({"F": stream(Some("first"), prefix, None)});
                if bad {
                    resources["Bad"] = failing_queue("F");
                    after["DependsOn"] = json!("Bad");
                } else {
                    after["DependsOn"] = json!("F");
                }
                resources["After"] = after;
                json!({"Resources": resources})
            };
            create(&svc, "s", &template("v1/", false)).await;
            update_held(&svc, "s", &template("v2/", true)).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_FAILED");
            assert_eq!(prefix_of(&svc, "first"), "v2/");
            assert_eq!(prefix_of(&svc, "after"), "v1/", "never reached");

            update(&svc, "s", &template("v2/", false)).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_COMPLETE");
            assert_eq!(prefix_of(&svc, "after"), "v2/", "the retry reaches it");
        }

        /// The same with nothing applied at all: a stop before the first resource
        /// must not make the attempt's template look applied.
        #[tokio::test]
        async fn a_retry_after_a_validation_stop_applies_everything() {
            let svc = make_service();
            create(
                &svc,
                "other",
                &json!({"Resources": {"T": stream(Some("held-name"), "a/", None)}}),
            )
            .await;
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "v1/", None)}}),
            )
            .await;
            let colliding = json!({"Resources": {
                "F": stream(Some("one"), "v2/", None),
                "New": stream(Some("held-name"), "a/", None),
            }});
            update_held(&svc, "s", &colliding).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_FAILED");
            assert_eq!(prefix_of(&svc, "one"), "v1/");

            update(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "v2/", None)}}),
            )
            .await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_COMPLETE");
            assert_eq!(prefix_of(&svc, "one"), "v2/");
        }

        /// Review finding: a retry's undo went back to the held attempt's
        /// definition, which was never applied.
        #[tokio::test]
        async fn a_retrys_rollback_reaches_the_last_stable_state() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "v1/", None)}}),
            )
            .await;
            // This attempt fails on F itself, so F stays at v1.
            let mut failing = stream(Some("one"), "v2/", None);
            failing["Properties"]["ExtendedS3DestinationConfiguration"]["CompressionFormat"] =
                json!("BROTLI");
            update_held(&svc, "s", &json!({"Resources": {"F": failing}})).await;
            assert_eq!(stack(&svc, "s").status, "UPDATE_FAILED");
            assert_eq!(prefix_of(&svc, "one"), "v1/");

            // The retry applies F, then fails on something else and rolls back.
            update(
                &svc,
                "s",
                &json!({"Resources": {
                    "F": stream(Some("one"), "v3/", None),
                    "Bad": failing_queue("F"),
                }}),
            )
            .await;
            let after = stack(&svc, "s");
            assert_eq!(after.status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(prefix_of(&svc, "one"), "v1/");
            assert!(
                after.template.contains("v1/"),
                "the stable template: {}",
                after.template
            );
        }

        fn nested(child: Value) -> Value {
            json!({
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {"TemplateBody": child.to_string()}
            })
        }

        fn child_with(prefix: &str, bad: bool) -> Value {
            let mut resources = json!({"F": stream(Some("inner"), prefix, None)});
            if bad {
                resources["Bad"] = failing_queue("F");
            }
            json!({"Resources": resources})
        }

        fn child_id(svc: &CloudFormationService, parent: &str) -> String {
            stack(svc, parent)
                .resources
                .iter()
                .find(|r| r.logical_id == "Child")
                .unwrap()
                .physical_id
                .clone()
        }

        fn stack_by_id(svc: &CloudFormationService, id: &str) -> Stack {
            svc.state
                .read()
                .regional(ACCOUNT, REGION)
                .unwrap()
                .stacks
                .values()
                .find(|s| s.stack_id == id)
                .unwrap()
                .clone()
        }

        /// Review finding: a nested stack's imports outlived it, blocking its
        /// exporter for good.
        #[tokio::test]
        async fn deleting_a_nested_stack_releases_what_it_imported() {
            let svc = make_service();
            create(
                &svc,
                "exporter",
                &json!({
                    "Resources": {"T": {"Type": "AWS::SNS::Topic"}},
                    "Outputs": {"O": {"Value": "x", "Export": {"Name": "shared"}}}
                }),
            )
            .await;
            let importing = json!({"Resources": {"Q": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"Tags": [{"Key": "k", "Value": {"Fn::ImportValue": "shared"}}]}
            }}});
            let anchor = json!({"Type": "AWS::SNS::Topic"});
            create(&svc, "parent", &json!({"Resources": {"Anchor": anchor}})).await;
            update(
                &svc,
                "parent",
                &json!({"Resources": {"Anchor": anchor, "Child": nested(importing)}}),
            )
            .await;
            assert_eq!(stack(&svc, "parent").status, "UPDATE_COMPLETE");
            assert!(svc
                .state
                .read()
                .regional(ACCOUNT, REGION)
                .unwrap()
                .imports
                .contains_key("shared"));

            update(&svc, "parent", &json!({"Resources": {"Anchor": anchor}})).await;
            assert!(!svc
                .state
                .read()
                .regional(ACCOUNT, REGION)
                .unwrap()
                .imports
                .contains_key("shared"));

            call(&svc, "DeleteStack", &[("StackName", "exporter")]).await;
            assert!(svc
                .state
                .read()
                .regional(ACCOUNT, REGION)
                .unwrap()
                .exports
                .is_empty());
        }

        /// Review finding: a nested create took over another stack's export.
        #[tokio::test]
        async fn a_nested_create_cannot_take_another_stacks_export() {
            let svc = make_service();
            create(
                &svc,
                "owner",
                &json!({
                    "Resources": {"T": {"Type": "AWS::SNS::Topic"}},
                    "Outputs": {"O": {"Value": "ours", "Export": {"Name": "shared"}}}
                }),
            )
            .await;
            let anchor = json!({"Type": "AWS::SNS::Topic"});
            create(&svc, "parent", &json!({"Resources": {"Anchor": anchor}})).await;
            let child = json!({
                "Resources": {"Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "child-must-not-exist"}
                }},
                "Outputs": {"O": {"Value": "theirs", "Export": {"Name": "shared"}}}
            });
            update(
                &svc,
                "parent",
                &json!({"Resources": {"Anchor": anchor, "Child": nested(child)}}),
            )
            .await;
            let parent = stack(&svc, "parent");
            assert_eq!(parent.status, "UPDATE_ROLLBACK_COMPLETE");
            assert!(
                crate::service::tests::failure_reasons(&svc, &parent.stack_id)
                    .iter()
                    .any(|r| r.contains("was not successfully created")
                        && r.contains(
                            "Export with name shared is already exported by stack owner"
                        ))
            );
            let accounts = svc.state.read();
            assert_eq!(
                accounts.regional(ACCOUNT, REGION).unwrap().exports["shared"].value,
                "ours"
            );
            drop(accounts);
            assert!(
                !svc.deps
                    .sqs
                    .read()
                    .regional(ACCOUNT, REGION)
                    .is_some_and(|s| s.name_to_url.contains_key("child-must-not-exist")),
                "nothing provisioned"
            );
        }

        /// Review finding: a child stuck in its rollback was recorded as if nothing
        /// was left to undo.
        #[test]
        fn a_child_that_kept_its_journal_stays_in_the_parents_undo() {
            let existing = StackResource {
                logical_id: "Child".to_string(),
                physical_id: "arn:child".to_string(),
                resource_type: STACK_TYPE.to_string(),
                status: "UPDATE_COMPLETE".to_string(),
                service_token: None,
                attributes: BTreeMap::new(),
                deletion_policy: None,
                update_replace_policy: None,
                custom_properties: None,
            };
            for status in ["UPDATE_FAILED", "UPDATE_ROLLBACK_FAILED"] {
                assert!(
                    matches!(
                        failed_child_entry(&existing, status),
                        JournalEntry::NestedUpdated { failed: true, .. }
                    ),
                    "{status}"
                );
            }
            assert!(matches!(
                failed_child_entry(&existing, "UPDATE_ROLLBACK_COMPLETE"),
                JournalEntry::UpdateFailed { previous: None, .. }
            ));
        }

        async fn parent_held_on_child(svc: &CloudFormationService) -> String {
            create(
                svc,
                "p",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let anchor = json!({"Type": "AWS::SNS::Topic"});
            update(
                svc,
                "p",
                &json!({"Resources": {"Anchor": anchor, "Child": nested(child_with("v1/", false))}}),
            )
            .await;
            assert_eq!(stack(svc, "p").status, "UPDATE_COMPLETE");
            let child = child_id(svc, "p");
            update_held(
                svc,
                "p",
                &json!({"Resources": {"Anchor": anchor, "Child": nested(child_with("v2/", true))}}),
            )
            .await;
            assert_eq!(stack(svc, "p").status, "UPDATE_FAILED");
            assert_eq!(stack_by_id(svc, &child).status, "UPDATE_FAILED");
            child
        }

        /// Review finding: a rollback reported success over a broken child, and
        /// ignored `ResourcesToSkip` and `ForceRollback` for it.
        #[tokio::test]
        async fn a_broken_child_stops_the_parents_rollback_until_told_otherwise() {
            let svc = make_service();
            let child = parent_held_on_child(&svc).await;
            remove_stream(&svc, "inner");
            call(&svc, "RollbackStack", &[("StackName", "p")]).await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_ROLLBACK_FAILED");
            assert_eq!(stack_by_id(&svc, &child).status, "UPDATE_ROLLBACK_FAILED");

            // Skipping the child's resource by its dotted name reaches the child.
            call(
                &svc,
                "ContinueUpdateRollback",
                &[("StackName", "p"), ("ResourcesToSkip.member.1", "Child.F")],
            )
            .await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(stack_by_id(&svc, &child).status, "UPDATE_ROLLBACK_COMPLETE");
        }

        #[tokio::test]
        async fn skipping_the_child_itself_leaves_it_be() {
            let svc = make_service();
            let child = parent_held_on_child(&svc).await;
            remove_stream(&svc, "inner");
            call(&svc, "RollbackStack", &[("StackName", "p")]).await;
            call(
                &svc,
                "ContinueUpdateRollback",
                &[("StackName", "p"), ("ResourcesToSkip.member.1", "Child")],
            )
            .await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(stack_by_id(&svc, &child).status, "UPDATE_ROLLBACK_FAILED");
        }

        #[tokio::test]
        async fn force_rollback_reaches_the_child() {
            let svc = make_service();
            let child = parent_held_on_child(&svc).await;
            remove_stream(&svc, "inner");
            call(&svc, "RollbackStack", &[("StackName", "p")]).await;
            call(
                &svc,
                "ContinueUpdateRollback",
                &[("StackName", "p"), ("ForceRollback", "true")],
            )
            .await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(stack_by_id(&svc, &child).status, "UPDATE_ROLLBACK_COMPLETE");
        }

        fn parent_after_child(prefix: &str, bad: bool) -> Value {
            let mut resources = json!({"Child": nested(child_with(prefix, false))});
            if bad {
                resources["Bad"] = failing_queue("Child");
            }
            json!({"Resources": resources})
        }

        /// Review finding: a parent held after its child succeeded left the child in
        /// a status the parent's retry could not claim.
        #[tokio::test]
        async fn a_parents_retry_can_change_a_child_waiting_for_cleanup() {
            let svc = make_service();
            create(
                &svc,
                "p",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            update(&svc, "p", &parent_after_child("v1/", false)).await;
            let child = child_id(&svc, "p");
            update_held(&svc, "p", &parent_after_child("v2/", true)).await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_FAILED");
            assert_eq!(
                stack_by_id(&svc, &child).status,
                "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"
            );

            update(&svc, "p", &parent_after_child("v3/", false)).await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_COMPLETE");
            let after = stack_by_id(&svc, &child);
            assert_eq!(after.status, "UPDATE_COMPLETE");
            assert!(after.pending_update.is_none());
            assert_eq!(prefix_of(&svc, "inner"), "v3/");
        }

        #[tokio::test]
        async fn a_parents_rollback_undoes_a_child_waiting_for_cleanup() {
            let svc = make_service();
            create(
                &svc,
                "p",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            update(&svc, "p", &parent_after_child("v1/", false)).await;
            let child = child_id(&svc, "p");
            update_held(&svc, "p", &parent_after_child("v2/", true)).await;
            call(&svc, "RollbackStack", &[("StackName", "p")]).await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(stack_by_id(&svc, &child).status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(prefix_of(&svc, "inner"), "v1/");
        }

        /// Review finding: the release of one attempt's invokes covered the next
        /// attempt too.
        #[tokio::test]
        async fn a_retrys_rollback_silences_only_what_was_never_released() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let anchor = json!({"Type": "AWS::SNS::Topic"});

            // The first attempt holds, releasing its custom resource's Create.
            let (first, outcome) = run_held(
                &svc,
                "s",
                &json!({"Resources": {
                    "Anchor": anchor,
                    "Seen": custom("a"),
                    "Bad": failing_queue("Seen"),
                }}),
            );
            assert_eq!(outcome.status, "UPDATE_FAILED");
            assert_eq!(first.pending_custom_invokes.lock().len(), 1, "released");

            // The retry queues another Create, then fails and rolls back. Only the
            // released resource's handler hears anything: its Delete.
            let (second, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {
                    "Anchor": anchor,
                    "Seen": custom("a"),
                    "Unseen": custom("b"),
                    "Bad": {
                        "Type": "AWS::SQS::Queue",
                        "DependsOn": ["Seen", "Unseen"],
                        "Properties": {"QueueName": "not a valid name!"}
                    },
                }}),
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            let invokes = second.pending_custom_invokes.lock();
            assert_eq!(invokes.len(), 1, "{invokes:?}");
            let event: Value = serde_json::from_str(&invokes[0].payload).unwrap();
            assert_eq!(event["RequestType"], "Delete");
            assert_eq!(event["LogicalResourceId"], "Seen");
            assert!(second.pending_container_spawns.lock().is_empty());
        }

        /// Review finding: a restart dropped the journal of a child its held parent
        /// still needs.
        #[tokio::test]
        async fn a_restart_keeps_the_journal_of_a_child_a_held_parent_waits_on() {
            let svc = make_service();
            create(
                &svc,
                "p",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            update(&svc, "p", &parent_after_child("v1/", false)).await;
            let child = child_id(&svc, "p");
            update_held(&svc, "p", &parent_after_child("v2/", true)).await;

            {
                let mut accounts = svc.state.write();
                settle_interrupted_updates(accounts.regional_get_mut(ACCOUNT, REGION).unwrap());
            }
            let waiting = stack_by_id(&svc, &child);
            assert_eq!(waiting.status, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS");
            assert!(waiting.pending_update.is_some());

            call(&svc, "RollbackStack", &[("StackName", "p")]).await;
            assert_eq!(prefix_of(&svc, "inner"), "v1/");

            // With no parent waiting and no journal, the same status settles.
            let mut lone = stack_by_id(&svc, &child);
            lone.status = "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS".to_string();
            lone.pending_update = None;
            let mut accounts = svc.state.write();
            let state = accounts.regional_get_mut(ACCOUNT, REGION).unwrap();
            let child_name = lone.name.clone();
            state.stacks.insert(child_name.clone(), lone);
            settle_interrupted_updates(state);
            assert_eq!(state.stacks[&child_name].status, "UPDATE_COMPLETE");
        }

        /// Review finding: with the post-provision check gone, two creates could
        /// both pass the pre-flight and the slower took the export. The window is
        /// simulated: the rival registers after this stack provisioned.
        #[tokio::test]
        async fn a_create_losing_an_export_race_rolls_back_what_it_provisioned() {
            let svc = make_service();
            let template = json!({
                "Resources": {"Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "lost-race"}
                }},
                "Outputs": {"O": {"Value": "mine", "Export": {"Name": "raced"}}}
            });
            // The stack as CreateStack seeds it, already provisioned.
            let body = template.to_string();
            let stack_id = "arn:aws:cloudformation:us-east-1:123456789012:stack/late/1".to_string();
            let provisioner = svc.scratch_provisioner(&stack_id, ACCOUNT, REGION);
            let parsed = template::parse_template(&body, &BTreeMap::new()).unwrap();
            let resources: Vec<StackResource> = parsed
                .resources
                .iter()
                .map(|def| provisioner.create_resource(def).unwrap())
                .collect();
            {
                let mut accounts = svc.state.write();
                let state = accounts.regional_mut(ACCOUNT, REGION);
                let seeded = Stack {
                    name: "late".to_string(),
                    stack_id: stack_id.clone(),
                    template: body.clone(),
                    status: "CREATE_IN_PROGRESS".to_string(),
                    status_reason: None,
                    resources: Vec::new(),
                    parameters: BTreeMap::new(),
                    tags: BTreeMap::new(),
                    created_at: Utc::now(),
                    updated_at: None,
                    description: None,
                    notification_arns: Vec::new(),
                    outputs: Vec::new(),
                    enable_termination_protection: false,
                    pending_update: None,
                    cancel_requested: false,
                };
                state.stacks.insert("late".to_string(), seeded);
                state.exports.insert(
                    "raced".to_string(),
                    crate::state::StackExport {
                        value: "theirs".to_string(),
                        exporting_stack_id: "arn:stack/rival/1".to_string(),
                        exporting_stack_name: "rival".to_string(),
                    },
                );
            }
            let outputs = CloudFormationService::resolve_template_outputs(
                &body,
                &BTreeMap::new(),
                &resources,
                &svc.state,
                ACCOUNT,
                REGION,
            );
            let reason = CloudFormationService::commit_created_stack(
                &svc.state,
                ACCOUNT,
                REGION,
                "late",
                &stack_id,
                &resources,
                &outputs,
                &[],
            )
            .expect("the rival holds the export");
            assert_eq!(
                reason,
                "Export with name raced is already exported by stack rival"
            );
            assert_eq!(stack(&svc, "late").status, "ROLLBACK_IN_PROGRESS");
            CloudFormationService::roll_back_created_stack(
                &svc.state,
                provisioner,
                ACCOUNT,
                REGION,
                "late",
                &stack_id,
                resources,
            )
            .await;

            let after = stack(&svc, "late");
            assert_eq!(after.status, "ROLLBACK_COMPLETE");
            assert!(after.resources.is_empty());
            assert!(!svc
                .deps
                .sqs
                .read()
                .regional(ACCOUNT, REGION)
                .is_some_and(|s| s.name_to_url.contains_key("lost-race")));
            let accounts = svc.state.read();
            let state = accounts.regional(ACCOUNT, REGION).unwrap();
            assert_eq!(state.exports["raced"].value, "theirs");
            let log: Vec<&str> = state.events[&stack_id]
                .iter()
                .map(|e| e["ResourceStatus"].as_str().unwrap())
                .collect();
            assert_eq!(
                log,
                [
                    "CREATE_IN_PROGRESS",
                    "CREATE_COMPLETE",
                    "ROLLBACK_IN_PROGRESS",
                    "DELETE_IN_PROGRESS",
                    "DELETE_COMPLETE",
                    "ROLLBACK_COMPLETE"
                ]
            );
        }

        /// The child's own rollback fails during the parent's forward pass: the
        /// stream it has to re-apply disappears just then. The parent must not
        /// report success over it, and the journal it kept finishes the job.
        #[tokio::test]
        async fn a_childs_rollback_failing_during_the_parents_update_stops_the_parent() {
            let svc = make_service();
            create(
                &svc,
                "p",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let anchor = json!({"Type": "AWS::SNS::Topic"});
            update(
                &svc,
                "p",
                &json!({"Resources": {"Anchor": anchor, "Child": nested(child_with("v1/", false))}}),
            )
            .await;
            let child = child_id(&svc, "p");

            let hooked = svc.clone();
            test_hooks::set(move |point| {
                if point == "undo:F" {
                    remove_stream(&hooked, "inner");
                }
            });
            update(
                &svc,
                "p",
                &json!({"Resources": {"Anchor": anchor, "Child": nested(child_with("v2/", true))}}),
            )
            .await;
            test_hooks::clear();

            let parent = stack(&svc, "p");
            assert_eq!(parent.status, "UPDATE_ROLLBACK_FAILED");
            assert_eq!(
                parent.status_reason.as_deref(),
                Some("The following resource(s) failed to update: [Child]. ")
            );
            assert!(parent.pending_update.is_some());
            let stuck = stack_by_id(&svc, &child);
            assert_eq!(stuck.status, "UPDATE_ROLLBACK_FAILED");
            assert!(stuck.pending_update.is_some());
            let reasons = crate::service::tests::failure_reasons(&svc, &parent.stack_id);
            assert!(
                reasons
                    .iter()
                    .any(|r| r.contains("Currently in UPDATE_ROLLBACK_FAILED")),
                "{reasons:?}"
            );
            assert!(
                reasons
                    .iter()
                    .any(|r| r.contains("was not successfully rolled back")),
                "{reasons:?}"
            );

            call(
                &svc,
                "ContinueUpdateRollback",
                &[("StackName", "p"), ("ForceRollback", "true")],
            )
            .await;
            assert_eq!(stack(&svc, "p").status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(stack_by_id(&svc, &child).status, "UPDATE_ROLLBACK_COMPLETE");
        }

        /// What a snapshot written at `point` holds, for the restart half of a test.
        fn snapshot_at(
            svc: &CloudFormationService,
            point: &'static str,
        ) -> std::rc::Rc<std::cell::RefCell<Option<Vec<u8>>>> {
            let taken = std::rc::Rc::new(std::cell::RefCell::new(None));
            let (slot, hooked) = (taken.clone(), svc.clone());
            test_hooks::set(move |at| {
                if at == point && slot.borrow().is_none() {
                    let accounts = hooked.state.read();
                    *slot.borrow_mut() = Some(serde_json::to_vec(&*accounts).unwrap());
                }
            });
            taken
        }

        /// Restart: the state as restored from `bytes`, over the service's other
        /// state, then the startup resume.
        async fn restart_from(svc: &CloudFormationService, bytes: &[u8]) {
            let mut restored: fakecloud_core::multi_account::MultiAccountState<
                crate::state::CloudFormationAccountState,
            > = serde_json::from_slice(bytes).unwrap();
            crate::restore_stack_sets(&mut restored);
            *svc.state.write() = restored;
            svc.resume_interrupted_cleanups().await;
        }

        fn topics(svc: &CloudFormationService) -> Vec<String> {
            let sns = svc.deps.sns.read();
            sns.regional(ACCOUNT, REGION)
                .map(|state| state.topics.keys().cloned().collect())
                .unwrap_or_default()
        }

        /// A restart during an update's cleanup finishes deleting what it was
        /// deleting: both the removed resource and the replaced one.
        #[tokio::test]
        async fn a_restart_finishes_an_updates_cleanup() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {
                    "Gone": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "gone"}},
                    "Also": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "also"}},
                    "F": stream(None, "a/", Some("arn:aws:kinesis:us-east-1:123456789012:stream/one")),
                }}),
            )
            .await;
            let old_stream = stack(&svc, "s")
                .resources
                .iter()
                .find(|r| r.logical_id == "F")
                .unwrap()
                .physical_id
                .clone();
            let taken = snapshot_at(&svc, "cleanup:Also");
            update(
                &svc,
                "s",
                &json!({"Resources": {
                    "F": stream(None, "a/", Some("arn:aws:kinesis:us-east-1:123456789012:stream/two")),
                }}),
            )
            .await;
            test_hooks::clear();
            let bytes = taken.borrow().clone().expect("reached the cleanup");
            let after_real_cleanup = stack(&svc, "s").resources[0].physical_id.clone();

            // Back to the moment of the snapshot, service state included.
            let new_stream = after_real_cleanup;
            let provisioner = svc.scratch_provisioner("x", ACCOUNT, REGION);
            let recreated = provisioner
                .create_resource(&ResourceDefinition {
                    logical_id: "Also".to_string(),
                    resource_type: "AWS::SNS::Topic".to_string(),
                    properties: json!({"TopicName": "also"}),
                    deletion_policy: None,
                    update_replace_policy: None,
                })
                .unwrap();
            assert!(topics(&svc).contains(&recreated.physical_id));
            assert_ne!(old_stream, new_stream);

            restart_from(&svc, &bytes).await;
            let settled = stack(&svc, "s");
            assert_eq!(settled.status, "UPDATE_COMPLETE");
            assert!(settled.pending_update.is_none());
            assert_eq!(settled.resources.len(), 1);
            assert_eq!(settled.resources[0].physical_id, new_stream);
            assert!(
                !topics(&svc).iter().any(|t| t.ends_with(":also")),
                "the interrupted cleanup still deleted it"
            );
        }

        /// The same for a rollback's cleanup, which also has a baseline to restore.
        #[tokio::test]
        async fn a_restart_finishes_a_rollbacks_cleanup() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "v1/", None)}}),
            )
            .await;
            let before = stack(&svc, "s");
            let taken = snapshot_at(&svc, "cleanup:Added");
            update(
                &svc,
                "s",
                &json!({"Resources": {
                    "F": stream(Some("one"), "v2/", None),
                    "Added": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "added"}},
                    "Bad": {
                        "Type": "AWS::SQS::Queue",
                        "DependsOn": ["F", "Added"],
                        "Properties": {"QueueName": "not a valid name!"}
                    },
                }}),
            )
            .await;
            test_hooks::clear();
            let bytes = taken.borrow().clone().expect("reached the cleanup");

            // Back to that moment: the topic the cleanup had yet to delete exists.
            svc.scratch_provisioner("x", ACCOUNT, REGION)
                .create_resource(&ResourceDefinition {
                    logical_id: "Added".to_string(),
                    resource_type: "AWS::SNS::Topic".to_string(),
                    properties: json!({"TopicName": "added"}),
                    deletion_policy: None,
                    update_replace_policy: None,
                })
                .unwrap();

            restart_from(&svc, &bytes).await;
            let settled = stack(&svc, "s");
            assert_eq!(settled.status, "UPDATE_ROLLBACK_COMPLETE");
            assert!(settled.pending_update.is_none());
            assert_eq!(settled.template, before.template);
            assert_eq!(settled.resources.len(), 1);
            assert!(!topics(&svc).iter().any(|t| t.ends_with(":added")));
        }

        fn exporting_child(export: &str) -> Value {
            json!({
                "Resources": {"T": {"Type": "AWS::SNS::Topic"}},
                "Outputs": {"O": {"Value": "x", "Export": {"Name": export}}}
            })
        }

        fn importer_of(export: &str) -> Value {
            json!({"Resources": {"Q": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"Tags": [{"Key": "k", "Value": {"Fn::ImportValue": export}}]}
            }}})
        }

        /// A nested child's exports hold its parent's delete like the parent's own,
        /// and an import inside the family does not count.
        #[tokio::test]
        async fn a_nested_childs_export_in_use_blocks_the_parents_delete() {
            let svc = make_service();
            let body = json!({"Resources": {
                "Child": nested(exporting_child("kid-export")),
                "Sibling": nested(importer_of("kid-export")),
            }});
            create(&svc, "family", &body).await;
            assert_eq!(stack(&svc, "family").status, "CREATE_COMPLETE");
            create(&svc, "outsider", &importer_of("kid-export")).await;

            call(&svc, "DeleteStack", &[("StackName", "family")]).await;
            let parent = stack(&svc, "family");
            assert_eq!(parent.status, "CREATE_COMPLETE");
            assert!(svc.state.read().regional(ACCOUNT, REGION).unwrap().events[&parent.stack_id]
                .iter()
                .any(|e| e["ResourceStatusReason"]
                    == "Delete canceled. Cannot delete export kid-export as it is in use by outsider."));

            call(&svc, "DeleteStack", &[("StackName", "outsider")]).await;
            call(&svc, "DeleteStack", &[("StackName", "family")]).await;
            assert_eq!(stack(&svc, "family").status, "DELETE_COMPLETE");
            let accounts = svc.state.read();
            let state = accounts.regional(ACCOUNT, REGION).unwrap();
            assert!(state.exports.is_empty());
            assert!(state.imports.is_empty());
        }

        /// Removing a nested stack from its parent is the same delete.
        #[tokio::test]
        async fn removing_a_nested_child_with_an_export_in_use_rolls_back() {
            let svc = make_service();
            let anchor = json!({"Type": "AWS::SNS::Topic"});
            create(
                &svc,
                "family",
                &json!({"Resources": {"Anchor": anchor, "Child": nested(exporting_child("kid-export"))}}),
            )
            .await;
            create(&svc, "outsider", &importer_of("kid-export")).await;
            update(&svc, "family", &json!({"Resources": {"Anchor": anchor}})).await;
            let parent = stack(&svc, "family");
            assert_eq!(parent.status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(parent.resources.len(), 2, "the child is still there");
            assert!(svc.state.read().regional(ACCOUNT, REGION).unwrap().events[&parent.stack_id]
                .iter()
                .any(|e| e["ResourceStatusReason"]
                    == "Delete canceled. Cannot delete export kid-export as it is in use by outsider."));
        }

        /// With the lock released, another stack can claim an export name between
        /// the pre-flight and the commit. The commit sees it, undoes the update,
        /// and leaves the other stack's export alone.
        #[tokio::test]
        async fn an_export_claimed_during_the_update_rolls_it_back() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Q": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let (_, outcome) = run_with(
                &svc,
                "s",
                &json!({
                    "Resources": {
                        "Q": {"Type": "AWS::SNS::Topic"},
                        "Added": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "added"}}
                    },
                    "Outputs": {"O": {"Value": "v", "Export": {"Name": "raced"}}}
                }),
                |svc| {
                    let mut accounts = svc.state.write();
                    accounts
                        .regional_get_mut(ACCOUNT, REGION)
                        .unwrap()
                        .exports
                        .insert(
                            "raced".to_string(),
                            crate::state::StackExport {
                                value: "theirs".to_string(),
                                exporting_stack_id: "arn:stack/other/1".to_string(),
                                exporting_stack_name: "other".to_string(),
                            },
                        );
                },
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(
                outcome.reason.as_deref(),
                Some("Export with name raced is already exported by stack other")
            );
            let after = stack(&svc, "s");
            assert_eq!(after.resources.len(), 1, "the added topic is undone");
            let accounts = svc.state.read();
            assert_eq!(
                accounts.regional(ACCOUNT, REGION).unwrap().exports["raced"].exporting_stack_name,
                "other"
            );
        }

        /// A cancel accepted while the update runs takes effect at the next
        /// resource boundary. Recorded: `User Initiated` on the rollback, no
        /// resource touched when nothing had started.
        #[tokio::test]
        async fn a_cancel_rolls_back_at_the_next_boundary() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "a/", None)}}),
            )
            .await;
            let (_, outcome) = run_with(
                &svc,
                "s",
                &json!({"Resources": {"F": stream(Some("one"), "b/", None)}}),
                |svc| {
                    let accounts_ok = {
                        use fakecloud_core::service::AwsService;
                        let handle = svc.handle(make_request(
                            "CancelUpdateStack",
                            params(&[("StackName", "s")]),
                        ));
                        futures_lite_block(handle)
                    };
                    assert!(accounts_ok.is_ok());
                },
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(statuses(&svc, "s", "F").len(), 2, "create events only");
            let firehose = svc.deps.firehose.read();
            let stored = &firehose.get(ACCOUNT).unwrap().streams(REGION).unwrap()["one"];
            assert_eq!(
                stored.destination.as_ref().unwrap().prefix.as_deref(),
                Some("a/")
            );
            drop(firehose);
            assert!(!stack(&svc, "s").cancel_requested);
        }

        /// A request handled inline: `CancelUpdateStack` never awaits.
        fn futures_lite_block<F: std::future::Future>(future: F) -> F::Output {
            let waker = std::task::Waker::noop();
            let mut context = std::task::Context::from_waker(waker);
            let mut future = std::pin::pin!(future);
            match future.as_mut().poll(&mut context) {
                std::task::Poll::Ready(output) => output,
                std::task::Poll::Pending => panic!("the handler should not suspend"),
            }
        }

        /// A rollback's `Update` request tells the handler what it is moving from.
        #[tokio::test]
        async fn a_custom_resources_undo_names_its_in_flight_properties() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let anchor = json!({"Type": "AWS::SNS::Topic"});
            run_on(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": anchor, "Thing": custom("a")}}),
            );
            run_held(
                &svc,
                "s",
                &json!({"Resources": {
                    "Anchor": anchor,
                    "Thing": custom("b"),
                    "Bad": failing_queue("Thing"),
                }}),
            );
            let held = stack(&svc, "s");
            assert_eq!(held.status, "UPDATE_FAILED");

            let provisioner = svc.provisioner_deferred(&held.stack_id, ACCOUNT, REGION);
            let engine = UpdateEngine::new(
                &svc.state,
                &provisioner,
                ACCOUNT,
                REGION,
                &held.stack_id,
                "s",
            );
            let outcome = engine.roll_back(
                held.pending_update.clone().unwrap(),
                RollbackOptions::default(),
                None,
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            let invokes = provisioner.pending_custom_invokes.lock();
            assert_eq!(invokes.len(), 1, "{invokes:?}");
            let event: Value = serde_json::from_str(&invokes[0].payload).unwrap();
            assert_eq!(event["RequestType"], "Update");
            assert_eq!(event["ResourceProperties"]["Value"], "a");
            assert_eq!(event["OldResourceProperties"]["Value"], "b");
        }

        /// Recorded: a changed service token fails with the bare text.
        #[tokio::test]
        async fn changing_a_service_token_is_refused_and_rolled_back() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let anchor = json!({"Type": "AWS::SNS::Topic"});
            run_on(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": anchor, "Thing": custom("a")}}),
            );
            let mut other = custom("a");
            other["Properties"]["ServiceToken"] =
                json!("arn:aws:lambda:us-east-1:123456789012:function:other");
            let (_, outcome) = run_on(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": anchor, "Thing": other}}),
            );
            assert_eq!(outcome.status, "UPDATE_ROLLBACK_COMPLETE");
            let current = stack(&svc, "s");
            assert_eq!(
                crate::service::tests::failure_reasons(&svc, &current.stack_id),
                ["Modifying service token is not allowed."]
            );
        }

        /// The floating versions are refused wherever a group names a template,
        /// and on update as on create.
        #[tokio::test]
        async fn floating_launch_template_versions_are_refused_everywhere() {
            let lt = json!({"Type": "AWS::EC2::LaunchTemplate", "Properties": {
                "LaunchTemplateData": {"ImageId": "ami-1"}
            }});
            let template = |group: Value| {
                json!({"Resources": {"L": lt, "R": {
                    "Type": "AWS::AutoScaling::AutoScalingGroup",
                    "Properties": group
                }}})
            };
            let base =
                || json!({"MinSize": "0", "MaxSize": "1", "AvailabilityZones": ["us-east-1a"]});
            let spec =
                |version: &str| json!({"LaunchTemplateId": {"Ref": "L"}, "Version": version});
            let mixed = |version: &str, override_version: &str| {
                let mut group = base();
                group["MixedInstancesPolicy"] = json!({"LaunchTemplate": {
                    "LaunchTemplateSpecification": spec(version),
                    "Overrides": [{"LaunchTemplateSpecification": spec(override_version)}]
                }});
                group
            };
            let plain = |version: &str| {
                let mut group = base();
                group["LaunchTemplate"] = spec(version);
                group
            };
            let reason = "CloudFormation does not support using $Latest or $Default";
            // Recorded: on a create, as the handler's message.
            for bad in [mixed("$Latest", "1"), mixed("1", "$Default")] {
                let svc = make_service();
                create(&svc, "s", &json!({"Resources": {"L": lt}})).await;
                update(&svc, "s", &template(bad)).await;
                let after = stack(&svc, "s");
                assert_eq!(after.status, "UPDATE_ROLLBACK_COMPLETE");
                assert!(
                    crate::service::tests::failure_reasons(&svc, &after.stack_id)
                        .iter()
                        .any(|r| r.contains(reason))
                );
            }
            // An existing group's update. Recorded: the bare text, with no
            // in-progress event before it or the rollback's completion after it.
            let svc = make_service();
            create(&svc, "s", &template(plain("1"))).await;
            assert_eq!(stack(&svc, "s").status, "CREATE_COMPLETE");
            update(&svc, "s", &template(plain("$Latest"))).await;
            let after = stack(&svc, "s");
            assert_eq!(after.status, "UPDATE_ROLLBACK_COMPLETE");
            assert_eq!(
                crate::service::tests::failure_reasons(&svc, &after.stack_id),
                [
                    "CloudFormation does not support using $Latest or $Default for LaunchTemplate version.  Please specify a version number or name."
                ]
            );
            let group_events = statuses(&svc, "s", "R");
            assert_eq!(
                group_events[group_events.len() - 2..],
                ["UPDATE_FAILED", "UPDATE_COMPLETE"]
            );
            assert_eq!(
                group_events
                    .iter()
                    .filter(|status| *status == "UPDATE_IN_PROGRESS")
                    .count(),
                0
            );
        }

        /// A delete that fails during a created stack's rollback is not reported as
        /// done: the stack says so and keeps the resource.
        #[tokio::test]
        async fn a_failed_delete_leaves_a_created_stacks_rollback_failed() {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"T": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            let current = stack(&svc, "s");
            let resource = current.resources[0].clone();
            let status = CloudFormationService::finish_created_rollback(
                &svc.state,
                ACCOUNT,
                REGION,
                "s",
                &current.stack_id,
                vec![(resource, Some(Err("still attached".to_string())))],
            );
            assert_eq!(status, "ROLLBACK_FAILED");
            let after = stack(&svc, "s");
            assert_eq!(after.status, "ROLLBACK_FAILED");
            assert_eq!(after.resources.len(), 1);
            assert_eq!(
                after.status_reason.as_deref(),
                Some("The following resource(s) failed to delete: [T]. ")
            );
            assert!(statuses(&svc, "s", "T").contains(&"DELETE_FAILED".to_string()));
            call(&svc, "DeleteStack", &[("StackName", "s")]).await;
            assert_eq!(stack(&svc, "s").status, "DELETE_COMPLETE");
        }

        /// An import a `Fn::ForEach` generates counts as an import.
        #[tokio::test]
        async fn imports_generated_by_for_each_are_recorded() {
            let svc = make_service();
            create(
                &svc,
                "exporter",
                &json!({
                    "Resources": {"T": {"Type": "AWS::SNS::Topic"}},
                    "Outputs": {"O": {"Value": "x", "Export": {"Name": "made-a"}}}
                }),
            )
            .await;
            let importer = json!({
                "Transform": "AWS::LanguageExtensions",
                "Resources": {"Fn::ForEach::Queues": ["Which", ["a"], {
                    "Q${Which}": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"Tags": [{"Key": "k", "Value": {
                            "Fn::ImportValue": {"Fn::Sub": "made-${Which}"}
                        }}]}
                    }
                }]}
            });
            create(&svc, "importer", &importer).await;
            assert_eq!(stack(&svc, "importer").status, "CREATE_COMPLETE");
            assert_eq!(
                svc.state.read().regional(ACCOUNT, REGION).unwrap().imports["made-a"],
                ["importer"]
            );
        }

        /// Recorded: updates of conditional create-only properties that keep the
        /// physical id, on named resources too, so neither a replacement nor the
        /// custom-name failure.
        #[tokio::test]
        async fn recorded_conditional_paths_keep_the_physical_id() {
            let instance = |props: Value| {
                let mut all = json!({"ImageId": "ami-1"});
                all.as_object_mut()
                    .unwrap()
                    .extend(props.as_object().unwrap().clone());
                json!({"Resources": {"R": {"Type": "AWS::EC2::Instance", "Properties": all}}})
            };
            let database = |retention: u32| {
                json!({"Resources": {"R": {
                    "Type": "AWS::RDS::DBInstance",
                    "Properties": {
                        "DBInstanceIdentifier": "named-db",
                        "DBInstanceClass": "db.t3.micro",
                        "Engine": "postgres",
                        "MasterUsername": "admin",
                        "MasterUserPassword": "password1234",
                        "AllocatedStorage": "20",
                        "BackupRetentionPeriod": retention
                    }
                }}})
            };
            let group = |subnet: &str, data: &str, version: &str| {
                json!({"Resources": {
                    "V": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.9.0.0/16"}},
                    "S1": {"Type": "AWS::EC2::Subnet", "Properties": {
                        "VpcId": {"Ref": "V"}, "CidrBlock": "10.9.1.0/24"
                    }},
                    "S2": {"Type": "AWS::EC2::Subnet", "Properties": {
                        "VpcId": {"Ref": "V"}, "CidrBlock": "10.9.2.0/24"
                    }},
                    "L": {"Type": "AWS::EC2::LaunchTemplate", "Properties": {
                        "LaunchTemplateData": {"ImageId": data}
                    }},
                    "R": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {
                        "AutoScalingGroupName": "named-group",
                        "MinSize": "0",
                        "MaxSize": "1",
                        "DesiredCapacity": "0",
                        "VPCZoneIdentifier": [{"Ref": subnet}],
                        "LaunchTemplate": {
                            "LaunchTemplateId": {"Ref": "L"},
                            "Version": version
                        }
                    }}
                }})
            };
            for (case, before, after) in [
                (
                    "InstanceType",
                    instance(json!({"InstanceType": "t3.micro"})),
                    instance(json!({"InstanceType": "t3.small"})),
                ),
                (
                    "UserData",
                    instance(json!({"UserData": "YQ=="})),
                    instance(json!({"UserData": "Yg=="})),
                ),
                ("BackupRetentionPeriod", database(1), database(7)),
                (
                    "LaunchTemplate",
                    group("S1", "ami-1", "1"),
                    group("S1", "ami-2", "2"),
                ),
                (
                    "VPCZoneIdentifier",
                    group("S1", "ami-1", "1"),
                    group("S2", "ami-1", "1"),
                ),
            ] {
                let svc = make_service();
                create(&svc, "s", &before).await;
                assert_eq!(stack(&svc, "s").status, "CREATE_COMPLETE", "{case}");
                let physical = |svc: &CloudFormationService| {
                    stack(svc, "s")
                        .resources
                        .iter()
                        .find(|r| r.logical_id == "R")
                        .unwrap()
                        .physical_id
                        .clone()
                };
                let old = physical(&svc);
                update(&svc, "s", &after).await;
                assert_eq!(stack(&svc, "s").status, "UPDATE_COMPLETE", "{case}");
                assert_eq!(physical(&svc), old, "{case}");
                assert!(
                    !statuses(&svc, "s", "R").contains(&"DELETE_IN_PROGRESS".to_string()),
                    "{case}"
                );
            }
        }

        /// Recorded: the API takes `$Latest` and `$Default`, CloudFormation refuses
        /// them when the group is created.
        #[tokio::test]
        async fn an_auto_scaling_group_refuses_a_floating_launch_template_version() {
            for version in ["$Latest", "$Default"] {
                let svc = make_service();
                create(
                    &svc,
                    "s",
                    &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
                )
                .await;
                update(
                    &svc,
                    "s",
                    &json!({"Resources": {
                        "Anchor": {"Type": "AWS::SNS::Topic"},
                        "L": {"Type": "AWS::EC2::LaunchTemplate", "Properties": {
                            "LaunchTemplateData": {"ImageId": "ami-1"}
                        }},
                        "R": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {
                            "MinSize": "0",
                            "MaxSize": "1",
                            "AvailabilityZones": ["us-east-1a"],
                            "LaunchTemplate": {
                                "LaunchTemplateId": {"Ref": "L"},
                                "Version": version
                            }
                        }}
                    }}),
                )
                .await;
                let after = stack(&svc, "s");
                assert_eq!(after.status, "UPDATE_ROLLBACK_COMPLETE", "{version}");
                assert!(crate::service::tests::failure_reasons(&svc, &after.stack_id)
                    .iter()
                    .any(|r| r.contains(
                        "CloudFormation does not support using $Latest or $Default for LaunchTemplate version.  Please specify a version number or name."
                    ) && r.contains("HandlerErrorCode: InvalidRequest")));
            }
        }

        /// A snapshot written before this engine carries `reapply_logical_ids`.
        #[test]
        fn an_old_snapshots_stack_still_loads() {
            let old = json!({
                "name": "s",
                "stack_id": "arn:aws:cloudformation:us-east-1:123456789012:stack/s/1",
                "template": "{}",
                "status": "UPDATE_ROLLBACK_COMPLETE",
                "resources": [],
                "parameters": {},
                "tags": {},
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": null,
                "description": null,
                "notification_arns": [],
                "reapply_logical_ids": ["A"]
            });
            let stack: Stack = serde_json::from_value(old).unwrap();
            assert!(stack.pending_update.is_none());
        }

        /// A journal survives a snapshot, so a held stack can roll back after a
        /// restart.
        #[tokio::test]
        async fn a_held_updates_journal_round_trips_through_serde() {
            let svc = make_service();
            held_stack(&svc).await;
            let held = stack(&svc, "s");
            let bytes = serde_json::to_vec(&held).unwrap();
            let restored: Stack = serde_json::from_slice(&bytes).unwrap();
            assert!(restored.pending_update.is_some());
            let absent = serde_json::to_value(stack(&make_service_stack().await, "s")).unwrap();
            assert!(absent.get("pending_update").is_none(), "normally absent");
        }

        async fn make_service_stack() -> CloudFormationService {
            let svc = make_service();
            create(
                &svc,
                "s",
                &json!({"Resources": {"Anchor": {"Type": "AWS::SNS::Topic"}}}),
            )
            .await;
            svc
        }
    }
}
