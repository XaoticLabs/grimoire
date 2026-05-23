//! Shared fixtures for the supervisor contract tests.

// Each test binary `mod common`s this file privately, so the lint sees its
// `pub` items as unreachable from that crate's root. The visibility is
// load-bearing — sibling test files need it.
#![allow(unreachable_pub)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;

use grimoire::daemon::clock::TestClock;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::supervisor::{EscalationMailSender, EscalationOutcome, Supervisor};
use grimoire::shared::types::{Agent, AgentState, RestartPolicy};

#[derive(Default)]
pub struct NoopMail;

#[async_trait]
impl EscalationMailSender for NoopMail {
    async fn send_escalation(&self, _: &str, _: &str, _: &str) -> Result<EscalationOutcome> {
        Ok(EscalationOutcome::default())
    }
}

pub fn seed(db: &Database, id: &str, state: AgentState) {
    let agent = Agent {
        id: id.to_string(),
        name: None,
        state,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: Some(1),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
}

pub fn build(db: Arc<Database>, bus: EventBus, clock: Arc<TestClock>) -> Arc<Supervisor> {
    let mail: Arc<dyn EscalationMailSender> = Arc::new(NoopMail);
    Supervisor::new(db, bus, clock, 30, 3, mail)
}
