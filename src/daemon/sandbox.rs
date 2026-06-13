//! Per-agent confinement: rewrites a spawn command to enforce a
//! [`SandboxConfig`] via `systemd-run` (resource caps) and `bwrap` (fs/net
//! jail). When a required binary isn't on `PATH` the helpers log once and
//! return the command unchanged, never failing the spawn.

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

/// True iff `systemd-run` is on `PATH`. Memoized; probed once per process.
fn systemd_run_available() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| binary_on_path("systemd-run"))
}

/// True iff `bwrap` is on `PATH`. Memoized; probed once per process.
fn bwrap_available() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| binary_on_path("bwrap"))
}

/// True iff the sandbox has any cgroup-enforced cap that systemd-run can apply.
const fn has_cgroup_limits(s: &SandboxConfig) -> bool {
    s.memory_max.is_some() || s.cpu_quota_percent.is_some()
}

/// Apply both sandbox layers, composing inside-out: `bwrap` (fs/net jail)
/// rewrites first so the `systemd-run` resource scope wraps the jailed binary.
/// Each layer no-ops when its host tool is missing.
pub fn apply(cmd: Command, sandbox: Option<&SandboxConfig>) -> Command {
    let cmd = apply_fs_jail(cmd, sandbox);
    apply_resource_limits(cmd, sandbox)
}

/// Wrap `cmd` in a transient systemd user scope with the requested resource
/// caps. Unchanged when nothing to enforce or `systemd-run` isn't installed.
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

    // tokio::Command can't read back stdio, but providers always use
    // null/piped; re-apply so wrapping doesn't change the contract.
    wrapped
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    wrapped
}

/// Wrap `cmd` in `bwrap` so it sees only an allow-listed filesystem and
/// (optionally) no network. Off unless `sandbox.fs_jail`; one-shot warning +
/// no-op if `bwrap` is missing.
///
/// Mounts: system dirs (`/usr`, `/etc`, `/lib`, …) read-only when present;
/// fresh `/proc` + `/dev`; tmpfs `/tmp` + `/run`; agent CWD read-write; plus
/// each `ro_paths`/`rw_paths` entry. `--die-with-parent` ties the jailed
/// process to the daemon so a crashed supervisor can't leak children.
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

    // Network: deny by default; `allow_network` opts in.
    if s.allow_network {
        wrapped.arg("--share-net");
    } else {
        wrapped.arg("--unshare-net");
    }

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

    // Agent CWD must be read-write so it can do work.
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

    // --setenv so vars survive bwrap's env clear.
    for (k, v) in &envs {
        if let Some(val) = v {
            wrapped.arg("--setenv").arg(k).arg(val);
        }
    }

    wrapped.arg("--").arg(program).args(args);

    // Re-apply env on the outer Command so a downstream wrapper (systemd-run)
    // inherits the same set.
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
