//! Per-agent artifact capture: a structured record of what an agent changed on
//! disk (files, unified diff, line counts) plus cost (tokens, USD), computed
//! from git when the agent finishes — so review/merge workflows treat the diff
//! as a first-class object. Best-effort: a non-git cwd, missing base, or git
//! error yields a cost-only artifact rather than failing the agent.
//!
//! Isolation gotcha: the diff is against the cwd's git state, so agents sharing
//! a working tree see each other's edits. Meaningful per-agent diffs need
//! per-agent isolation (a named workspace or dedicated worktree).

use std::path::Path;
use std::process::Command;

use crate::shared::types::{AgentArtifact, FileChange};

/// Cap on retained diff bytes (head kept, tail dropped) so a runaway agent
/// can't store a gigabyte of diff.
pub const MAX_DIFF_BYTES: usize = 256 * 1024;

/// `git -C <cwd> <args...>` → stdout on success, `None` on any failure.
/// `--no-optional-locks` avoids racing an agent's own git.
fn git(cwd: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `HEAD` commit of `cwd`, if it's a git work tree with a commit. Recorded at
/// dispatch as the baseline for the completion-time diff.
pub fn head_commit(cwd: &Path) -> Option<String> {
    let sha = git(cwd, &["rev-parse", "HEAD"])?;
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_string())
    }
}

/// Compute an agent's on-disk change: file list (porcelain status, catching
/// staged/unstaged/untracked), per-file + total line counts (numstat vs
/// `base`), and the capped unified diff (vs `base`).
///
/// `base` is the dispatch-time commit; `None` falls back to `HEAD` (so a repo
/// created mid-run still reports something). If that also fails, only the cost
/// fields are meaningful.
#[must_use]
pub fn compute(
    agent_id: &str,
    cwd: &Path,
    base: Option<&str>,
    tokens_used: u64,
    usd_spent: f64,
    captured_at: i64,
) -> AgentArtifact {
    let mut files: Vec<FileChange> = Vec::new();
    let mut insertions: u64 = 0;
    let mut deletions: u64 = 0;
    let mut diff_text: Option<String> = None;

    // Prefer `base`; fall back to HEAD so a non-baselined run still reports
    // tracked edits.
    let range = base
        .map(str::to_string)
        .or_else(|| head_commit(cwd))
        .unwrap_or_default();

    if !range.is_empty() {
        if let Some(numstat) = git(cwd, &["diff", "--numstat", &range]) {
            for line in numstat.lines() {
                let mut cols = line.split('\t');
                let (Some(add), Some(del), Some(path)) = (cols.next(), cols.next(), cols.next())
                else {
                    continue;
                };
                // Binary files report `-` for both columns.
                let a: u64 = add.parse().unwrap_or(0);
                let d: u64 = del.parse().unwrap_or(0);
                insertions += a;
                deletions += d;
                files.push(FileChange {
                    path: path.to_string(),
                    status: "M".to_string(),
                    insertions: a,
                    deletions: d,
                });
            }
        }

        if let Some(mut diff) = git(cwd, &["diff", &range]) {
            if diff.len() > MAX_DIFF_BYTES {
                let mut cut = diff.len() - MAX_DIFF_BYTES;
                while cut < diff.len() && !diff.is_char_boundary(cut) {
                    cut += 1;
                }
                diff = format!("[…{} bytes of diff truncated…]\n{}", cut, &diff[cut..]);
            }
            if !diff.trim().is_empty() {
                diff_text = Some(diff);
            }
        }
    }

    // Merge in porcelain status for untracked (`??`) and deletions that a
    // tracked-only numstat misses, preferring numstat counts on overlap.
    if let Some(status) = git(cwd, &["status", "--porcelain"]) {
        for line in status.lines() {
            if line.len() < 4 {
                continue;
            }
            let code = line[..2].trim();
            let path = line[3..].trim().to_string();
            if files.iter().any(|f| f.path == path) {
                continue;
            }
            files.push(FileChange {
                path,
                status: if code.is_empty() {
                    "M".to_string()
                } else {
                    code.to_string()
                },
                insertions: 0,
                deletions: 0,
            });
        }
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    AgentArtifact {
        agent_id: agent_id.to_string(),
        base_commit: base.map(str::to_string),
        files_changed: files,
        diff: diff_text,
        insertions,
        deletions,
        tokens_used,
        usd_spent,
        captured_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::process::Command;

    fn run(dir: &Path, args: &[&str]) {
        let ok = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .expect("git runs")
            .status
            .success();
        assert!(ok, "git {args:?} failed");
    }

    fn init_repo(dir: &Path) {
        run(dir, &["init", "-q"]);
        run(dir, &["config", "user.email", "t@t.t"]);
        run(dir, &["config", "user.name", "t"]);
        fs::write(dir.join("a.txt"), "one\ntwo\nthree\n").unwrap();
        run(dir, &["add", "."]);
        run(dir, &["commit", "-qm", "init"]);
    }

    #[test]
    fn non_git_cwd_yields_cost_only_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let a = compute("ag", dir.path(), None, 100, 0.01, 42);
        assert!(a.files_changed.is_empty());
        assert!(a.diff.is_none());
        assert_eq!(a.tokens_used, 100);
        assert_eq!(a.captured_at, 42);
    }

    #[test]
    fn captures_modified_and_untracked_files() {
        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        let base = head_commit(dir.path()).expect("repo has HEAD");

        // Modify a tracked file and add an untracked one.
        fs::write(dir.path().join("a.txt"), "one\ntwo\nthree\nfour\n").unwrap();
        fs::write(dir.path().join("b.txt"), "new file\n").unwrap();

        let a = compute("ag", dir.path(), Some(&base), 0, 0.0, 1);
        assert_eq!(a.base_commit.as_deref(), Some(base.as_str()));
        // a.txt gained a line; numstat reports it.
        let a_txt = a
            .files_changed
            .iter()
            .find(|f| f.path == "a.txt")
            .expect("a.txt tracked");
        assert_eq!(a_txt.insertions, 1);
        assert!(a.insertions >= 1);
        // b.txt is untracked; porcelain surfaces it as `??`.
        let b_txt = a
            .files_changed
            .iter()
            .find(|f| f.path == "b.txt")
            .expect("b.txt untracked");
        assert_eq!(b_txt.status, "??");
        // Diff text covers the tracked modification.
        let diff = a.diff.expect("tracked diff present");
        assert!(diff.contains("a.txt"));
        assert!(diff.contains("+four"));
    }
}
