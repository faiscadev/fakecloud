//! Pod label conventions shared by every FakeCloud k8s backend.
//!
//! Every Pod FakeCloud creates carries three labels so it can be found
//! and cleaned up later:
//! - [`MANAGED_BY`] = [`MANAGED_BY_VALUE`] marks the Pod as FakeCloud's.
//! - [`INSTANCE`] holds the owning process identity (`fakecloud-<pid>`),
//!   so a restarted server can tell its own Pods from a dead run's.
//! - [`SERVICE`] holds which service owns it (`lambda`, `elasticache`,
//!   `rds`, `ecs`) so each service reaps only its own kind of Pod.

/// Label key marking a Pod as managed by FakeCloud.
pub const MANAGED_BY: &str = "fakecloud-managed-by";
/// Value for [`MANAGED_BY`].
pub const MANAGED_BY_VALUE: &str = "fakecloud";
/// Label key holding the owning process identity (`fakecloud-<pid>`).
pub const INSTANCE: &str = "fakecloud-instance";
/// Label key holding the owning service name.
pub const SERVICE: &str = "fakecloud-service";

/// The per-process instance identity used for the [`INSTANCE`] label.
/// Stable for the lifetime of the process; changes across restarts so
/// reaping can distinguish a new run's Pods from a dead run's.
pub fn instance_id() -> String {
    format!("fakecloud-{}", std::process::id())
}
