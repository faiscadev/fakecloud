//! `EcsRuntime` `config` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcsRuntime {
    /// Path suitable for `DOCKER_CONFIG`. `None` if the tempdir setup
    /// failed; in that case pulls fall back to the user's own config and
    /// will only work if they've already logged in.
    pub(super) fn docker_config_path(&self) -> Option<PathBuf> {
        self.docker_config.as_ref().map(|d| d.path().to_path_buf())
    }

    /// Build a `Command` for the container CLI with `DOCKER_CONFIG` set
    /// to our isolated tempdir so fakecloud ECR auth works out of the box.
    pub(super) fn cli_command(&self) -> Command {
        let mut cmd = Command::new(&self.cli);
        if let Some(p) = self.docker_config_path() {
            cmd.env("DOCKER_CONFIG", p);
        }
        cmd
    }

    /// Wire SecretsManager state so `secrets[].valueFrom` entries
    /// pointing at SecretsManager ARNs resolve at task launch.
    pub fn with_secretsmanager(mut self, state: SharedSecretsManagerState) -> Self {
        self.secretsmanager_state = Some(state);
        self
    }

    /// Wire SSM state so `secrets[].valueFrom` entries pointing at
    /// Parameter Store ARNs resolve at task launch.
    pub fn with_ssm(mut self, state: SharedSsmState) -> Self {
        self.ssm_state = Some(state);
        self
    }
}
