//! Workflow variables (`Assign`, `$name`), in both query languages.
//!
//! A state machine's top-level states share one scope. Each Parallel branch and
//! Map iteration runs in a scope of its own that can read outer scopes, except a
//! distributed Map's, which cannot. New values take effect when the assigning
//! state finishes: every reference in a state reads the values from state entry.

use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};

tokio::task_local! {
    static CURRENT: Arc<VariableScope>;
}

#[derive(Default)]
pub(crate) struct VariableScope {
    parent: Option<Arc<VariableScope>>,
    values: Mutex<Map<String, Value>>,
    pending: Mutex<Map<String, Value>>,
}

impl VariableScope {
    pub(crate) fn root() -> Arc<Self> {
        Arc::default()
    }

    fn lookup(&self, name: &str) -> Option<Value> {
        let own = self
            .values
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned();
        own.or_else(|| self.parent.as_ref()?.lookup(name))
    }

    fn collect_into(&self, out: &mut Map<String, Value>) {
        if let Some(parent) = &self.parent {
            parent.collect_into(out);
        }
        let values = self.values.lock().unwrap_or_else(|e| e.into_inner());
        for (name, value) in values.iter() {
            out.insert(name.clone(), value.clone());
        }
    }
}

/// The scope a Parallel branch or Map iteration starts: nested in the current
/// scope, or for a distributed Map's iterations standing alone.
pub(crate) fn child_scope(can_read_outer: bool) -> Arc<VariableScope> {
    let parent = can_read_outer
        .then(|| CURRENT.try_with(Arc::clone).ok())
        .flatten();
    Arc::new(VariableScope {
        parent,
        ..VariableScope::default()
    })
}

/// Run a future inside a scope.
pub(crate) async fn in_scope<F: std::future::Future>(
    scope: Arc<VariableScope>,
    fut: F,
) -> F::Output {
    CURRENT.scope(scope, fut).await
}

/// A variable's value, read from the current scope outward.
pub(crate) fn lookup(name: &str) -> Option<Value> {
    CURRENT.try_with(|scope| scope.lookup(name)).ok().flatten()
}

/// Every variable visible from the current scope.
pub(crate) fn visible() -> Map<String, Value> {
    let mut out = Map::new();
    let _ = CURRENT.try_with(|scope| scope.collect_into(&mut out));
    out
}

/// Stage a value for the end of the current state.
pub(crate) fn stage(name: String, value: Value) {
    let _ = CURRENT.try_with(|scope| {
        scope
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name, value);
    });
}

/// Make the current state's assignments visible to the next state.
pub(crate) fn commit() {
    let _ = CURRENT.try_with(|scope| {
        let staged = std::mem::take(&mut *scope.pending.lock().unwrap_or_else(|e| e.into_inner()));
        scope
            .values
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(staged);
    });
}

/// The variable a path addresses and the path within it: `$order.items[0]` is
/// `order` and `$.items[0]`. `None` for `$`, `$.x`, `$[0]` and `$$...`.
pub(crate) fn split_reference(path: &str) -> Option<(&str, String)> {
    let rest = path.strip_prefix('$')?;
    let end = rest
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    Some((&rest[..end], format!("${}", &rest[end..])))
}

/// A JSONPath state's `Assign`: a payload template over `value`, whose meaning
/// depends on the state type, and the context object.
pub(crate) fn assign_jsonpath(
    assign: Option<&Value>,
    value: &Value,
    context: Option<&Value>,
) -> Result<(), (String, String)> {
    let Some(template) = assign else {
        return Ok(());
    };
    let Value::Object(evaluated) = crate::interpreter::apply_parameters(template, value, context)?
    else {
        return Ok(());
    };
    for (name, value) in evaluated {
        stage(name, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run<F: std::future::Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    #[test]
    fn references_split_into_variable_and_path() {
        assert_eq!(
            split_reference("$order.items[0]"),
            Some(("order", "$.items[0]".to_string()))
        );
        assert_eq!(split_reference("$x"), Some(("x", "$".to_string())));
        for not_variable in ["$", "$.x", "$[0]", "$$.Execution", "x"] {
            assert_eq!(split_reference(not_variable), None, "{not_variable}");
        }
    }

    #[test]
    fn assignments_take_effect_when_the_state_finishes() {
        run(in_scope(VariableScope::root(), async {
            stage("x".to_string(), json!(1));
            assert_eq!(lookup("x"), None);
            commit();
            assert_eq!(lookup("x"), Some(json!(1)));
        }));
    }

    #[test]
    fn nested_scopes_read_outward_and_keep_their_own_values() {
        run(in_scope(VariableScope::root(), async {
            stage("outer".to_string(), json!(1));
            commit();

            let child = child_scope(true);
            in_scope(child, async {
                assert_eq!(lookup("outer"), Some(json!(1)));
                stage("inner".to_string(), json!(2));
                commit();
                assert_eq!(lookup("inner"), Some(json!(2)));
            })
            .await;
            assert_eq!(lookup("inner"), None);

            let distributed = child_scope(false);
            in_scope(distributed, async {
                assert_eq!(lookup("outer"), None);
            })
            .await;
        }));
    }

    #[test]
    fn sibling_scopes_do_not_share() {
        run(in_scope(VariableScope::root(), async {
            in_scope(child_scope(true), async {
                stage("a".to_string(), json!(1));
                commit();
            })
            .await;
            in_scope(child_scope(true), async {
                assert_eq!(lookup("a"), None);
            })
            .await;
        }));
    }
}
