// Test-only helper for the durable-work-queue spec (Task 12).
//
// Polls the database for an agent's state, returning when it matches a
// target state or timing out with the actual final state. Use this in any
// test where work goes through `enqueue + scheduler.tick_now()` rather than
// the old synchronous `summon` path — post-enqueue the agent is `Queued`,
// so a direct `assert_eq!(state, Active)` would race the scheduler.

use std::time::Duration;

use anyhow::{Result, anyhow};
use grimoire::daemon::persistence::Database;
use grimoire::shared::types::{Agent, AgentState};

const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Poll `db.get_agent(id)` every 25ms until the agent's state equals
/// `target`. Returns the agent on success, or an error after `timeout`
/// whose message names the actual final state for fast triage.
#[allow(dead_code)]
pub async fn wait_for_state(
    db: &Database,
    id: &str,
    target: AgentState,
    timeout: Duration,
) -> Result<Agent> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let Some(agent) = db.get_agent(id)? else {
            return Err(anyhow!("wait_for_state: agent {id} not found"));
        };
        if agent.state == target {
            return Ok(agent);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "wait_for_state({id}, {:?}, {:?}) timed out; final state was {:?}",
                target,
                timeout,
                agent.state,
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
