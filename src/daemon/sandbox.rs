//! Per-agent confinement: rewrites a spawn command to enforce a
//! [`SandboxConfig`] when host tooling is available.
//!
//! Today this layer handles cgroup-style resource caps via
//! `systemd-run --user --scope`. Filesystem isolation via `bwrap` is
//! layered on top in a follow-up. When the required binary isn't on
//! `PATH` the helpers degrade gracefully: they log once and return
//! the original command, never failing the spawn.

use std::sync::OnceLock;

use tokio::process::Command;
use tracing::warn;

use crate::shared::config::SandboxConfig;

/// Return true if `name` resolves to an executable on the current `PATH`.
pub(crate) fn binary_on_path(name: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| {
        let candidate = dir.join(name);
        std::fs::metadata(&candidate).is_ok_and(|m| m.is_file())
    })
}

/// True iff `systemd-run` is on `PATH`. Memoized — probed once per process.
fn systemd_run_available() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| binary_on_path("systemd-run"))
}

/// True iff `bwrap` is on `PATH`. Memoized — probed once per process.
fn bwrap_available() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| binary_on_path("bwrap"))
}

/// True iff the sandbox has any cgroup-enforced cap that systemd-run can apply.
const fn has_cgroup_limits(s: &SandboxConfig) -> bool {
    s.memory_max.is_some() || s.cpu_quota_percent.is_some()
}

/// Apply every layer of the sandbox config to `cmd`:
///   1. Filesystem / network jail via `bwrap` (when `fs_jail` is set).
///   2. Resource caps (memory, CPU) via `systemd-run --user --scope`.
///
/// Layers compose inside-out: bwrap rewrites first (so the resource scope
/// wraps the jailed binary), then systemd-run is layered on the outside.
/// Each layer no-ops cleanly when its host tool is missing.
pub fn apply(cmd: Command, sandbox: Option<&SandboxConfig>) -> Command {
    let cmd = apply_fs_jail(cmd, sandbox);
    apply_resource_limits(cmd, sandbox)
}

/// Wrap `cmd` so the underlying binary runs inside a transient systemd
/// user scope with the requested resource caps. Returns the original
/// command unchanged when there is nothing to enforce or when
/// `systemd-run` isn't installed.
pub fn apply_resource_limits(cmd: Command, sandbox: Option<&SandboxConfig>) -> Command {
    let Some(s) = sandbox else { return cmd };
    if !has_cgroup_limits(s) {
        return cmd;
    }
    if !systemd_run_available() {
        static WARNED: OnceLock<()> = OnceLock::new();
        WARNED.get_or_init(|| {
            warn!(
                "sandbox: cgroup limits requested but `systemd-run` not found on PATH; \
                 spawning agent unconfined"
            );
        });
        return cmd;
    }
    wrap_with_systemd_run(cmd, s)
}

fn wrap_with_systemd_run(cmd: Command, s: &SandboxConfig) -> Command {
    let std_cmd = cmd.as_std();
    let program = std_cmd.get_program().to_os_string();
    let args: Vec<_> = std_cmd
        .get_args()
        .map(std::ffi::OsStr::to_os_string)
        .collect();
    let envs: Vec<(std::ffi::OsString, Option<std::ffi::OsString>)> = std_cmd
        .get_envs()
        .map(|(k, v)| (k.to_os_string(), v.map(std::ffi::OsStr::to_os_string)))
        .collect();
    let cwd = std_cmd.get_current_dir().map(std::path::Path::to_path_buf);

    let mut wrapped = Command::new("systemd-run");
    wrapped
        .arg("--user")
        .arg("--scope")
        .arg("--quiet")
        .arg("--collect");

    if let Some(mem) = s.memory_max {
        wrapped.arg(format!("--property=MemoryMax={mem}"));
    }
    if let Some(q) = s.cpu_quota_percent {
        wrapped.arg(format!("--property=CPUQuota={q}%"));
    }

    wrapped.arg("--").arg(program).args(args);

    if let Some(dir) = cwd {
        wrapped.current_dir(dir);
    }
    for (k, v) in envs {
        match v {
            Some(val) => {
                wrapped.env(&k, val);
                wrapped.arg(format!("--setenv={}", k.to_string_lossy()));
            }
            None => {
                wrapped.env_remove(&k);
            }
        }
    }

    // Inherit stdio config from the original by re-applying piped streams.
    // tokio::process::Command doesn't expose stdio readers, but providers
    // always set null/piped before spawn — re-applying the same here keeps
    // the contract identical regardless of wrapping.
    wrapped
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    wrapped
}

/// Wrap `cmd` in `bwrap` so the binary sees only an allow-listed view of the
/// filesystem and (optionally) no network. Off unless `sandbox.fs_jail` is set;
/// no-ops with a one-shot warning if `bwrap` isn't on `PATH`.
///
/// The jail mounts:
///   * `/usr`, `/etc`, `/lib`, `/lib64`, `/bin`, `/sbin` read-only (when they
///     exist on the host) — enough to exec a typical CLI and read system certs.
///   * `/proc` and `/dev` from fresh kernel filesystems.
///   * `tmpfs` at `/tmp` and `/run`.
///   * The agent's `current_dir` read-write.
///   * Each `sandbox.ro_paths` entry read-only and each `sandbox.rw_paths`
///     entry read-write.
///
/// `--die-with-parent` ties the jailed process to the daemon lifetime so a
/// crashed supervisor doesn't leak running children.
pub fn apply_fs_jail(cmd: Command, sandbox: Option<&SandboxConfig>) -> Command {
    let Some(s) = sandbox else { return cmd };
    if !s.fs_jail {
        return cmd;
    }
    if !bwrap_available() {
        static WARNED: OnceLock<()> = OnceLock::new();
        WARNED.get_or_init(|| {
            warn!(
                "sandbox: fs_jail requested but `bwrap` not found on PATH; \
                 spawning agent without filesystem isolation"
            );
        });
        return cmd;
    }
    wrap_with_bwrap(cmd, s)
}

fn wrap_with_bwrap(cmd: Command, s: &SandboxConfig) -> Command {
    let std_cmd = cmd.as_std();
    let program = std_cmd.get_program().to_os_string();
    let args: Vec<_> = std_cmd
        .get_args()
        .map(std::ffi::OsStr::to_os_string)
        .collect();
    let envs: Vec<(std::ffi::OsString, Option<std::ffi::OsString>)> = std_cmd
        .get_envs()
        .map(|(k, v)| (k.to_os_string(), v.map(std::ffi::OsStr::to_os_string)))
        .collect();
    let cwd = std_cmd.get_current_dir().map(std::path::Path::to_path_buf);

    let mut wrapped = Command::new("bwrap");
    wrapped.arg("--die-with-parent").arg("--new-session");

    // Network namespace: deny by default, share-net opts in.
    if s.allow_network {
        wrapped.arg("--share-net");
    } else {
        wrapped.arg("--unshare-net");
    }

    // System libs / config: bind read-only when present on host.
    for sys in ["/usr", "/etc", "/lib", "/lib64", "/bin", "/sbin"] {
        if std::path::Path::new(sys).exists() {
            wrapped.arg("--ro-bind").arg(sys).arg(sys);
        }
    }
    wrapped
        .arg("--proc")
        .arg("/proc")
        .arg("--dev")
        .arg("/dev")
        .arg("--tmpfs")
        .arg("/tmp")
        .arg("--tmpfs")
        .arg("/run");

    // Agent CWD is always read-write — the agent has to be able to do work.
    if let Some(dir) = &cwd {
        wrapped.arg("--bind").arg(dir).arg(dir);
        wrapped.arg("--chdir").arg(dir);
    }

    for p in &s.ro_paths {
        wrapped.arg("--ro-bind").arg(p).arg(p);
    }
    for p in &s.rw_paths {
        wrapped.arg("--bind").arg(p).arg(p);
    }

    // Pass env vars through with --setenv so they survive bwrap's env clear.
    for (k, v) in &envs {
        if let Some(val) = v {
            wrapped.arg("--setenv").arg(k).arg(val);
        }
    }

    wrapped.arg("--").arg(program).args(args);

    // Re-apply env on the outer Command too so any downstream wrapper
    // (systemd-run) inherits the same set.
    for (k, v) in envs {
        match v {
            Some(val) => {
                wrapped.env(k, val);
            }
            None => {
                wrapped.env_remove(k);
            }
        }
    }
    if let Some(dir) = cwd {
        wrapped.current_dir(dir);
    }
    wrapped
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    wrapped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_sandbox_returns_unchanged() {
        let cmd = Command::new("/bin/true");
        let wrapped = apply_resource_limits(cmd, None);
        assert_eq!(wrapped.as_std().get_program(), "/bin/true");
    }

    #[test]
    fn empty_sandbox_returns_unchanged() {
        let s = SandboxConfig::default();
        let cmd = Command::new("/bin/true");
        let wrapped = apply_resource_limits(cmd, Some(&s));
        assert_eq!(wrapped.as_std().get_program(), "/bin/true");
    }

    #[test]
    fn cgroup_limits_route_through_systemd_run_when_available() {
        if !systemd_run_available() {
            return; // CI without systemd: nothing to assert
        }
        let s = SandboxConfig {
            memory_max: Some(256 * 1024 * 1024),
            cpu_quota_percent: Some(50),
            ..SandboxConfig::default()
        };
        let cmd = Command::new("/bin/true");
        let wrapped = apply_resource_limits(cmd, Some(&s));
        assert_eq!(wrapped.as_std().get_program(), "systemd-run");
        let args: Vec<_> = wrapped
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(args.iter().any(|a| a == "--property=MemoryMax=268435456"));
        assert!(args.iter().any(|a| a == "--property=CPUQuota=50%"));
        assert!(args.iter().any(|a| a == "/bin/true"));
    }
}
