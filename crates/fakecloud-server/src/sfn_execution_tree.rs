//! `/_fakecloud/stepfunctions/execution-tree/{arn}`: an execution and the
//! executions its Tasks started, nested by `parent_execution_arn`.

use std::collections::HashMap;

use fakecloud_sdk::types::{StepFunctionsExecutionTreeNode, StepFunctionsExecutionTreeResponse};
use fakecloud_stepfunctions::{Execution, StepFunctionsState};

/// The tree rooted at `root_arn` in `state`, the account and region its ARN
/// names (a Task starts its children there too). `None` when the execution
/// does not exist.
pub(crate) fn execution_tree(
    state: &StepFunctionsState,
    root_arn: &str,
) -> Option<StepFunctionsExecutionTreeResponse> {
    let root = state.executions.get(root_arn)?;
    let mut children_by_parent: HashMap<&str, Vec<&Execution>> = HashMap::new();
    for exec in state.executions.values() {
        if let Some(parent) = exec.parent_execution_arn.as_deref() {
            children_by_parent.entry(parent).or_default().push(exec);
        }
    }
    Some(StepFunctionsExecutionTreeResponse {
        root_arn: root.execution_arn.clone(),
        tree: build_node(root, &children_by_parent, root_arn),
    })
}

/// Each execution has one parent, so only the root can be reached twice:
/// through a parent loop that state saved by an older build may hold. The walk
/// skips the root as a child so such a loop cannot recurse forever.
fn build_node(
    exec: &Execution,
    children_by_parent: &HashMap<&str, Vec<&Execution>>,
    root_arn: &str,
) -> StepFunctionsExecutionTreeNode {
    let mut children: Vec<&Execution> = children_by_parent
        .get(exec.execution_arn.as_str())
        .into_iter()
        .flatten()
        .copied()
        .filter(|child| child.execution_arn != root_arn)
        .collect();
    children.sort_by_key(|child| child.start_date);
    StepFunctionsExecutionTreeNode {
        arn: exec.execution_arn.clone(),
        state_machine_arn: exec.state_machine_arn.clone(),
        status: exec.status.as_str().to_string(),
        started_at: exec.start_date.to_rfc3339(),
        stopped_at: exec.stop_date.map(|d| d.to_rfc3339()),
        children: children
            .into_iter()
            .map(|child| build_node(child, children_by_parent, root_arn))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use fakecloud_stepfunctions::ExecutionStatus;

    fn arn(name: &str) -> String {
        format!("arn:aws:states:us-east-1:123456789012:execution:sm:{name}")
    }

    fn add(state: &mut StepFunctionsState, name: &str, parent: Option<&str>, offset_ms: i64) {
        let execution_arn = arn(name);
        state.executions.insert(
            execution_arn.clone(),
            Execution {
                execution_arn,
                state_machine_arn: "arn:aws:states:us-east-1:123456789012:stateMachine:sm"
                    .to_string(),
                state_machine_name: "sm".to_string(),
                name: name.to_string(),
                status: ExecutionStatus::Running,
                input: None,
                output: None,
                start_date: Utc::now() + Duration::milliseconds(offset_ms),
                stop_date: None,
                error: None,
                cause: None,
                history_events: vec![],
                parent_execution_arn: parent.map(arn),
                is_sync: false,
                billed_duration_ms: None,
                billed_memory_mb: None,
                role_arn: "arn:aws:iam::123456789012:role/r".to_string(),
                redrive_count: 0,
            },
        );
    }

    fn names(node: &StepFunctionsExecutionTreeNode) -> Vec<String> {
        node.children
            .iter()
            .map(|c| c.arn.rsplit(':').next().unwrap().to_string())
            .collect()
    }

    #[test]
    fn nests_children_in_start_order() {
        let mut state = StepFunctionsState::new("123456789012", "us-east-1");
        add(&mut state, "root", None, 0);
        add(&mut state, "late", Some("root"), 20);
        add(&mut state, "early", Some("root"), 10);
        add(&mut state, "grandchild", Some("early"), 30);
        add(&mut state, "unrelated", None, 40);

        let tree = execution_tree(&state, &arn("root")).unwrap();
        assert_eq!(tree.root_arn, arn("root"));
        assert_eq!(names(&tree.tree), ["early", "late"]);
        assert_eq!(names(&tree.tree.children[0]), ["grandchild"]);
    }

    #[test]
    fn unknown_execution_has_no_tree() {
        let state = StepFunctionsState::new("123456789012", "us-east-1");
        assert!(execution_tree(&state, &arn("missing")).is_none());
    }

    #[test]
    fn a_saved_self_parent_does_not_loop() {
        let mut state = StepFunctionsState::new("123456789012", "us-east-1");
        add(&mut state, "root", Some("root"), 0);
        let tree = execution_tree(&state, &arn("root")).unwrap();
        assert!(tree.tree.children.is_empty());
    }

    #[test]
    fn a_saved_loop_through_the_root_does_not_recurse() {
        // An older build re-parented on idempotent replies: root -> child -> root.
        let mut state = StepFunctionsState::new("123456789012", "us-east-1");
        add(&mut state, "root", Some("child"), 0);
        add(&mut state, "child", Some("root"), 10);
        let tree = execution_tree(&state, &arn("root")).unwrap();
        assert_eq!(names(&tree.tree), ["child"]);
        assert!(tree.tree.children[0].children.is_empty());
    }
}
