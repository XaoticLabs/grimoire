use std::collections::HashMap;

use anyhow::{Context, Result};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::cli::formatters;
use crate::shared::protocol::{CircleResult, EvalScoresResult};

pub async fn run(
    state: Option<String>,
    eval_score_lt: Option<f64>,
    eval_score_gte: Option<f64>,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let params = serde_json::json!({ "state": state });
    let response = client.call("agent.circle", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: CircleResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;

    let mut agents = result.agents;
    let need_scores = eval_score_lt.is_some() || eval_score_gte.is_some();
    if need_scores {
        let scores = fetch_scores(&mut client).await?;
        agents.retain(|a| {
            let Some(s) = scores.get(&a.id) else {
                return false;
            };
            if let Some(lt) = eval_score_lt
                && *s >= lt
            {
                return false;
            }
            if let Some(gte) = eval_score_gte
                && *s < gte
            {
                return false;
            }
            true
        });
        formatters::format_circle_with_scores(&agents, &scores);
    } else {
        formatters::format_circle(&agents);
    }

    Ok(())
}

async fn fetch_scores(client: &mut DaemonClient) -> Result<HashMap<String, f64>> {
    let response = client.call("eval.scores", serde_json::json!({})).await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: EvalScoresResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    Ok(result
        .scores
        .into_iter()
        .map(|e| (e.target_id, e.score))
        .collect())
}
