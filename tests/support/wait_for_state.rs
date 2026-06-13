#![allow(unreachable_pub)] // shared via `mod support` in each test crate; pub is load-bearing
// Polls the DB for an agent's state. Needed wherever work goes through
// `enqueue + tick_now()`: the agent is `Queued` post-enqueue, so a direct
// `assert_eq!(state, Active)` would race the scheduler.

use std::time::Duration;

use anyhow::{Result, anyhow};
use grimoire::daemon::persistence::Database;
use grimoire::shared::types::{Agent, AgentState};

const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Returns the agent once its state equals `target`, or errors after
/// `timeout` with a message naming the actual final state.
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
