//! `grim race` — fork a task into N competing variants, run each in its own
//! isolated git worktree, score every result against a rubric, and optionally
//! auto-merge the winner's branch by score.
//!
//! Orchestration lives CLI-side and composes the daemon's existing RPC
//! primitives (`agent.summon`/`result`/`artifact` + the shared eval prompt) so
//! the daemon stays stateless about races. Worktrees + per-variant branches use
//! plain `git` so each variant is isolated and the winner is a mergeable branch.

use anyhow::{Context, Result, anyhow, bail};
use colored::Colorize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::cli::client::DaemonClient;
use crate::shared::eval::{EvalVerdict, build_eval_prompt, fold_stdout_output, parse_verdict};
use crate::shared::protocol::{
    AgentArtifactResult, AgentResultResponse, ReplayResponse, SummonResult,
};

/// Substitute a variant into a task template: replaces the `{variant}`
/// placeholder, or appends the variant as an instruction if absent.
#[must_use]
pub fn substitute_variant(template: &str, variant: &str) -> String {
    if template.contains("{variant}") {
        template.replace("{variant}", variant)
    } else {
        format!("{template}\n\nUse this approach / variant: {variant}")
    }
}

/// Turn a variant label into a git-ref-safe segment.
#[must_use]
pub fn sanitize_branch(variant: &str) -> String {
    let s: String = variant
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = s.trim_matches('-');
    if trimmed.is_empty() {
        "variant".to_string()
    } else {
        trimmed.to_string()
    }
}

/// One variant's outcome after running + scoring.
#[derive(Debug, Clone)]
pub struct RaceResult {
    pub variant: String,
    pub agent_id: String,
    pub branch: String,
    pub worktree: PathBuf,
    /// `false` when the agent did not finish cleanly (failed/banished/timeout).
    pub completed: bool,
    pub score: f64,
    pub verdict: Option<String>,
    pub files_changed: usize,
    pub insertions: u64,
    pub deletions: u64,
    pub usd: f64,
    /// `true` when the variant produced a commit that can be merged.
    pub has_commit: bool,
}

/// Winning index by [`is_better`] total order. A non-completed variant only
/// wins if nothing else completed. `None` for an empty slice.
#[must_use]
pub fn pick_winner(results: &[RaceResult]) -> Option<usize> {
    if results.is_empty() {
        return None;
    }
    let mut best = 0usize;
    for i in 1..results.len() {
        if is_better(&results[i], &results[best]) {
            best = i;
        }
    }
    Some(best)
}

/// Total order used by `pick_winner`: prefer completed, then higher score,
/// then lower cost, then a mergeable commit.
fn is_better(a: &RaceResult, b: &RaceResult) -> bool {
    match (a.completed, b.completed) {
        (true, false) => return true,
        (false, true) => return false,
        _ => {}
    }
    if (a.score - b.score).abs() > 1e-9 {
        return a.score > b.score;
    }
    if (a.usd - b.usd).abs() > 1e-9 {
        return a.usd < b.usd;
    }
    a.has_commit && !b.has_commit
}

/// Run `git -C <repo> <args...>`, returning trimmed stdout. Errors carry the
/// stderr so worktree/merge failures are legible.
async fn git(repo: &Path, args: &[&str]) -> Result<String> {
    let out = tokio::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .await
        .with_context(|| format!("running git {args:?}"))?;
    if !out.status.success() {
        bail!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn is_terminal_state(state: &str) -> bool {
    matches!(
        state.to_ascii_lowercase().as_str(),
        "complete" | "failed" | "banished" | "dormant"
    )
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    task: &str,
    variants_csv: &str,
    repo: &Path,
    base_branch: &str,
    rubric_path: &str,
    provider: Option<String>,
    model: Option<String>,
    merge: bool,
    timeout_secs: u64,
    keep_worktrees: bool,
) -> Result<()> {
    let variants: Vec<String> = {
        let mut seen = std::collections::HashSet::new();
        variants_csv
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .filter(|s| seen.insert(s.to_string()))
            .map(String::from)
            .collect()
    };
    if variants.len() < 2 {
        bail!("race needs at least 2 distinct --variants (got {variants_csv:?})");
    }

    let rubric = std::fs::read_to_string(rubric_path)
        .with_context(|| format!("reading rubric file {rubric_path}"))?;
    if rubric.trim().is_empty() {
        bail!("rubric file {rubric_path} is empty");
    }

    let repo = std::fs::canonicalize(repo)
        .with_context(|| format!("repo path {} not found", repo.display()))?;
    if !repo.join(".git").exists() {
        bail!("{} is not a git repository", repo.display());
    }

    let race_id = crate::shared::constants::generate_short_id();
    let race_id = &race_id[..8.min(race_id.len())];
    println!(
        "{} race {} — {} variants off {}",
        "◆".bold(),
        race_id.bold(),
        variants.len(),
        base_branch.dimmed(),
    );

    let mut client = DaemonClient::connect().await?;

    // Worktree + agent per variant.
    let mut results: Vec<RaceResult> = Vec::new();
    let worktree_root = repo.join(".grim-race").join(race_id);
    for variant in &variants {
        let safe = sanitize_branch(variant);
        let branch = format!("race/{race_id}/{safe}");
        let worktree = worktree_root.join(&safe);
        git(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                &branch,
                &worktree.to_string_lossy(),
                base_branch,
            ],
        )
        .await
        .with_context(|| format!("creating worktree for variant {variant}"))?;

        let prompt = substitute_variant(task, variant);
        let resp = client
            .call(
                "agent.summon",
                serde_json::json!({
                    "task": prompt,
                    "name": format!("race:{race_id}:{safe}"),
                    "provider": provider,
                    "model": model,
                    "cwd": worktree,
                }),
            )
            .await?;
        if let Some(err) = resp.error {
            bail!("summon for variant {variant} failed: {}", err.message);
        }
        let summoned: SummonResult = serde_json::from_value(resp.result.unwrap_or_default())?;
        println!(
            "  {} {} → agent {} ({})",
            "↳".dimmed(),
            variant.bold(),
            summoned.id.dimmed(),
            branch.dimmed(),
        );
        results.push(RaceResult {
            variant: variant.clone(),
            agent_id: summoned.id,
            branch,
            worktree,
            completed: false,
            score: 0.0,
            verdict: None,
            files_changed: 0,
            insertions: 0,
            deletions: 0,
            usd: 0.0,
            has_commit: false,
        });
    }

    println!("  {} running… (timeout {}s)", "⏱".dimmed(), timeout_secs);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    for r in &mut results {
        r.completed = wait_terminal(&mut client, &r.agent_id, deadline).await?;
    }

    for r in &mut results {
        // Commit whatever the agent changed so the branch is mergeable.
        git(&r.worktree, &["add", "-A"]).await.ok();
        let dirty = !git(&r.worktree, &["status", "--porcelain"])
            .await
            .unwrap_or_default()
            .is_empty();
        if dirty {
            git(
                &r.worktree,
                &[
                    "commit",
                    "-m",
                    &format!("race {race_id}: variant {}", r.variant),
                ],
            )
            .await
            .ok();
            r.has_commit = true;
        }

        if let Ok(art) = client
            .call_typed::<AgentArtifactResult>(
                "agent.artifact",
                serde_json::json!({ "id": r.agent_id }),
            )
            .await
            && let Some(a) = art.artifact
        {
            r.files_changed = a.files_changed.len();
            r.insertions = a.insertions;
            r.deletions = a.deletions;
            r.usd = a.usd_spent;
        }

        match score_variant(
            &mut client,
            &r.agent_id,
            &rubric,
            provider.clone(),
            model.clone(),
            deadline,
        )
        .await
        {
            Ok(v) => {
                r.score = v.score;
                r.verdict = v.verdict;
            }
            Err(e) => {
                eprintln!(
                    "  {} scoring variant {} failed: {e}",
                    "⚠".yellow(),
                    r.variant
                );
            }
        }
    }

    print_results(&results);

    let Some(win_idx) = pick_winner(&results) else {
        bail!("no variants to choose from");
    };
    let winner = results[win_idx].clone();
    println!(
        "\n{} winner: {} (score {:.2}, branch {})",
        "★".green().bold(),
        winner.variant.bold(),
        winner.score,
        winner.branch.dimmed(),
    );

    if merge {
        if winner.has_commit {
            match git(
                &repo,
                &[
                    "merge",
                    "--no-ff",
                    "-m",
                    &format!("merge race {race_id} winner: {}", winner.variant),
                    &winner.branch,
                ],
            )
            .await
            {
                Ok(_) => println!(
                    "  {} merged {} into the current branch of {}",
                    "✓".green(),
                    winner.branch.bold(),
                    repo.display()
                ),
                Err(e) => println!("  {} merge failed (resolve manually): {e}", "✗".red()),
            }
        } else {
            println!(
                "  {} nothing to merge — winning variant produced no commit",
                "•".dimmed()
            );
        }
    } else {
        println!(
            "  {} re-run with {} to merge, or `git merge {}` yourself",
            "→".dimmed(),
            "--merge".dimmed(),
            winner.branch,
        );
    }

    // Clean up worktrees; branches are kept for inspection/merge.
    if keep_worktrees {
        println!(
            "  {} worktrees kept under {}",
            "•".dimmed(),
            worktree_root.display()
        );
    } else {
        for r in &results {
            git(
                &repo,
                &[
                    "worktree",
                    "remove",
                    "--force",
                    &r.worktree.to_string_lossy(),
                ],
            )
            .await
            .ok();
        }
    }

    Ok(())
}

/// Poll an agent until it reaches a terminal state or the deadline passes.
/// Returns whether it completed (vs. failed / timed out).
async fn wait_terminal(
    client: &mut DaemonClient,
    agent_id: &str,
    deadline: Instant,
) -> Result<bool> {
    loop {
        let resp: AgentResultResponse = client
            .call_typed("agent.result", serde_json::json!({ "id": agent_id }))
            .await?;
        if is_terminal_state(&resp.state) {
            return Ok(resp.state.eq_ignore_ascii_case("complete")
                || resp.state.eq_ignore_ascii_case("dormant"));
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Summon an evaluator over the variant agent's transcript and return its
/// parsed verdict. Mirrors `grim eval`'s scoring path.
async fn score_variant(
    client: &mut DaemonClient,
    agent_id: &str,
    rubric: &str,
    provider: Option<String>,
    model: Option<String>,
    deadline: Instant,
) -> Result<EvalVerdict> {
    let replay: ReplayResponse = client
        .call_typed("agent.replay", serde_json::json!({ "id": agent_id }))
        .await?;
    let max_seq = replay.entries.last().map_or(0, |e| e.seq);
    let transcript = fold_stdout_output(replay.entries.iter().map(|e| &e.event));
    let prompt = build_eval_prompt(agent_id, max_seq, rubric, &transcript);

    let resp = client
        .call(
            "agent.summon",
            serde_json::json!({
                "task": prompt,
                "name": format!("race-eval:{}", &agent_id[..8.min(agent_id.len())]),
                "provider": provider,
                "model": model,
            }),
        )
        .await?;
    if let Some(err) = resp.error {
        bail!("summon evaluator failed: {}", err.message);
    }
    let evaluator: SummonResult = serde_json::from_value(resp.result.unwrap_or_default())?;

    loop {
        let r: AgentResultResponse = client
            .call_typed("agent.result", serde_json::json!({ "id": evaluator.id }))
            .await?;
        if is_terminal_state(&r.state) {
            let text = r
                .result
                .ok_or_else(|| anyhow!("evaluator {} finished with no result", evaluator.id))?;
            return parse_verdict(&text);
        }
        if Instant::now() >= deadline {
            bail!(
                "evaluator {} did not finish before the deadline",
                evaluator.id
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn print_results(results: &[RaceResult]) {
    println!();
    println!(
        "  {:<16} {:>6} {:<8} {:>6} {:>9} {:>9}",
        "VARIANT".bold(),
        "SCORE".bold(),
        "VERDICT".bold(),
        "FILES".bold(),
        "LINES".bold(),
        "USD".bold(),
    );
    for r in results {
        let score = format!("{:.2}", r.score);
        let score = if r.score >= 0.8 {
            score.green()
        } else if r.score >= 0.5 {
            score.yellow()
        } else {
            score.red()
        };
        let state = if r.completed { "" } else { " (incomplete)" };
        println!(
            "  {:<16} {:>6} {:<8} {:>6} {:>9} {:>9}{}",
            r.variant,
            score,
            r.verdict.as_deref().unwrap_or("-"),
            r.files_changed,
            format!("+{}/-{}", r.insertions, r.deletions),
            format!("{:.4}", r.usd),
            state.dimmed(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn r(variant: &str, completed: bool, score: f64, usd: f64, commit: bool) -> RaceResult {
        RaceResult {
            variant: variant.into(),
            agent_id: "a".into(),
            branch: "b".into(),
            worktree: PathBuf::from("/tmp"),
            completed,
            score,
            verdict: None,
            files_changed: 0,
            insertions: 0,
            deletions: 0,
            usd,
            has_commit: commit,
        }
    }

    #[test]
    fn substitute_placeholder_then_fallback() {
        assert_eq!(
            substitute_variant("use {variant} for storage", "redis"),
            "use redis for storage"
        );
        assert!(substitute_variant("build it", "postgres").contains("postgres"));
    }

    #[test]
    fn sanitize_branch_is_ref_safe() {
        assert_eq!(sanitize_branch("redis"), "redis");
        assert_eq!(sanitize_branch("Postgres 15!"), "Postgres-15");
        assert_eq!(sanitize_branch("///"), "variant");
    }

    #[test]
    fn winner_is_highest_score() {
        let rs = vec![
            r("a", true, 0.4, 0.1, true),
            r("b", true, 0.9, 0.2, true),
            r("c", true, 0.7, 0.1, true),
        ];
        assert_eq!(pick_winner(&rs).unwrap(), 1);
    }

    #[test]
    fn winner_tiebreak_prefers_cheaper_then_commit() {
        let rs = vec![
            r("a", true, 0.8, 0.20, true),
            r("b", true, 0.8, 0.05, false),
        ];
        assert_eq!(pick_winner(&rs).unwrap(), 1);

        let rs2 = vec![
            r("a", true, 0.8, 0.10, false),
            r("b", true, 0.8, 0.10, true),
        ];
        assert_eq!(pick_winner(&rs2).unwrap(), 1);
    }

    #[test]
    fn completed_beats_incomplete_even_with_lower_score() {
        let rs = vec![
            r("crashed", false, 0.95, 0.0, false),
            r("finished", true, 0.30, 0.0, true),
        ];
        assert_eq!(pick_winner(&rs).unwrap(), 1);
    }

    #[test]
    fn empty_has_no_winner() {
        assert!(pick_winner(&[]).is_none());
    }

    // These guard paths bail before any daemon connection.
    #[tokio::test]
    async fn fewer_than_two_variants_is_rejected() {
        let err = run(
            "task",
            "only-one",
            Path::new("."),
            "HEAD",
            "/nonexistent/rubric.md",
            None,
            None,
            false,
            1,
            false,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("at least 2"), "{err}");
    }

    #[tokio::test]
    async fn missing_rubric_is_rejected() {
        let err = run(
            "task",
            "a,b",
            Path::new("."),
            "HEAD",
            "/nonexistent/rubric-file.md",
            None,
            None,
            false,
            1,
            false,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("rubric"), "{err}");
    }
}
