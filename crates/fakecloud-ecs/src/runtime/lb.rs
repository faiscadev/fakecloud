//! `EcsRuntime` `lb` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcsRuntime {
    pub(super) fn register_lb_targets(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
    ) {
        let Some(ref bus) = self.delivery_bus else {
            return;
        };
        let accounts = state.read();
        let Some(s) = accounts.get(account_id) else {
            return;
        };
        let Some(task) = s.tasks.get(task_id) else {
            return;
        };
        let targets = compute_elbv2_targets(s, task);
        drop(accounts);
        for (tg_arn, tg_targets) in targets {
            bus.register_elbv2_targets(account_id, &tg_arn, tg_targets);
        }
    }

    pub(super) fn deregister_lb_targets(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
    ) {
        let Some(ref bus) = self.delivery_bus else {
            return;
        };
        let accounts = state.read();
        let Some(s) = accounts.get(account_id) else {
            return;
        };
        let Some(task) = s.tasks.get(task_id) else {
            return;
        };
        let targets = compute_elbv2_targets(s, task);
        drop(accounts);
        for (tg_arn, tg_targets) in targets {
            bus.deregister_elbv2_targets(account_id, &tg_arn, tg_targets);
        }
    }
}
