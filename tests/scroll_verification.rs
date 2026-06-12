//! Integration tests for verification-gated scroll tasks.
//!
//! A task with a `verify:` rubric does not complete when its worker
//! finishes: the keeper summons an evaluator agent that scores the
//! worker's transcript against the rubric, and the DAG proceeds only
//! when the score clears the threshold. These tests drive the keeper
//! through the event bus exactly like the daemon does, with an
//! in-memory DB and no real provider processes.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use tokio::time::Duration;

use grimoire::daemon::agent_manager::AgentManager;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::scroll_parser;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{
    Agent, AgentEvent, AgentState, RestartPolicy, Scroll, ScrollState, Task, TaskState,
};

// ---------------------------------------------------------------------------
// Parser: verify / verify_threshold directives
// ---------------------------------------------------------------------------

#[test]
fn parser_reads_verify_and_threshold() {
    let content = r"# Scroll: Verified

## Task: Build
- verify: The build must succeed and all tests must pass.
- verify_threshold: 0.9

Build the project.
";
    let spec = scroll_parser::parse_scroll(content).unwrap();
    let task = &spec.tasks[0];
    assert_eq!(
        task.verify.as_deref(),
        Some("The build must succeed and all tests must pass.")
    );
    assert!((task.verify_threshold.unwrap() - 0.9).abs() < 1e-9);
}

#[test]
fn parser_rejects_bad_threshold() {
    for bad in ["1.5", "-0.1", "abc", "NaN"] {
        let content = format!(
            "# Scroll: Bad\n\n## Task: A\n- verify: rubric\n- verify_threshold: {bad}\n\nDo A.\n"
        );
        let err = scroll_parser::parse_scroll(&content).unwrap_err();
        assert!(
            err.to_string().contains("verify_threshold"),
            "threshold '{bad}' should be rejected, got: {err}"
        );
    }
}

#[test]
fn parser_defaults_to_no_verification() {
    let content = r"# Scroll: Plain

## Task: A

Do A.
";
    let spec = scroll_parser::parse_scroll(content).unwrap();
    assert!(spec.tasks[0].verify.is_none());
    assert!(spec.tasks[0].verify_threshold.is_none());
}

// ---------------------------------------------------------------------------
// Keeper: verification flow
// ---------------------------------------------------------------------------

const WORKER_ID: &str = "wrk00001";
const TASK_ID: &str = "tkv00001";
const DOWN_ID: &str = "tkv00002";
const SCROLL_ID: &str = "scv00001";

async fn setup() -> (Arc<Database>, EventBus) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let keeper = Arc::new(ScrollKeeper::new(db.clone(), manager));
    keeper.start(&bus);
    (db, bus)
}

fn seed_agent(db: &Database, id: &str, state: AgentState) {
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
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
}

/// Seed an active scroll with a verification-gated task (worker already
/// attached) and a downstream task that depends on it.
fn seed_verified_scroll(db: &Database, threshold: Option<f64>) {
    let now = Utc::now();
    db.insert_scroll(&Scroll {
        id: SCROLL_ID.into(),
        name: "Verified".into(),
        state: ScrollState::Active,
        source_path: None,
        max_concurrency: 4,
        created_at: now,
        updated_at: now,
    })
    .unwrap();
    let task = Task {
        id: TASK_ID.into(),
        scroll_id: SCROLL_ID.into(),
        name: "build".into(),
        prompt: "build it".into(),
        state: TaskState::Active,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 0,
        created_at: now,
        updated_at: now,
        peer_name: None,
        verify_rubric: Some("Did the build succeed?".into()),
        verify_threshold: threshold,
        verifier_agent_id: None,
    };
    let downstream = Task {
        id: DOWN_ID.into(),
        scroll_id: SCROLL_ID.into(),
        name: "deploy".into(),
        prompt: "deploy it".into(),
        state: TaskState::Blocked,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 1,
        created_at: now,
        updated_at: now,
        peer_name: None,
        verify_rubric: None,
        verify_threshold: None,
        verifier_agent_id: None,
    };
    db.insert_task(&task).unwrap();
    db.insert_task(&downstream).unwrap();
    db.insert_task_dependency(DOWN_ID, TASK_ID).unwrap();
    db.update_task_agent(TASK_ID, WORKER_ID).unwrap();
}

/// Publish the worker's completion and wait for the keeper to summon the
/// evaluator. Returns the evaluator's agent id.
async fn drive_to_verification(db: &Database, bus: &EventBus) -> String {
    bus.publish(StreamEvent::StateChange {
        agent_id: WORKER_ID.into(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    db.get_task(TASK_ID)
        .unwrap()
        .unwrap()
        .verifier_agent_id
        .expect("verifier agent should be recorded on the task")
}

/// Seed the evaluator's stdout with a claude-shaped `result` event whose
/// result body is `verdict_text`, then publish its completion.
async fn finish_verifier(db: &Database, bus: &EventBus, verifier_id: &str, verdict_text: &str) {
    let line = serde_json::json!({ "type": "result", "result": verdict_text }).to_string();
    db.insert_event(&AgentEvent {
        id: None,
        agent_id: verifier_id.to_string(),
        event_type: "stdout".into(),
        payload: line,
        created_at: Utc::now(),
    })
    .unwrap();
    bus.publish(StreamEvent::StateChange {
        agent_id: verifier_id.to_string(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn worker_completion_summons_evaluator_and_holds_task() {
    let (db, bus) = setup().await;
    seed_agent(&db, WORKER_ID, AgentState::Active);
    seed_verified_scroll(&db, None);

    // Give the worker some transcript for the evaluator prompt.
    db.append_event(&StreamEvent::Output {
        agent_id: WORKER_ID.into(),
        stream: "stdout".into(),
        line: "the build output looks healthy".into(),
    })
    .unwrap();

    let verifier_id = drive_to_verification(&db, &bus).await;

    // The task is held open, not completed.
    let task = db.get_task(TASK_ID).unwrap().unwrap();
    assert_eq!(task.state, TaskState::Active);

    // The evaluator is enqueued on the ad-hoc lane with rubric + transcript.
    let evaluator = db.get_agent(&verifier_id).unwrap().unwrap();
    assert_eq!(evaluator.state, AgentState::Queued);
    assert_eq!(evaluator.name.as_deref(), Some("verify:build"));
    let prompt = evaluator.task.unwrap();
    assert!(prompt.contains("Did the build succeed?"));
    assert!(prompt.contains("the build output looks healthy"));
    assert!(prompt.contains("\"score\""));

    let queue = db.list_queue().unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].lane, "adhoc");
    assert_eq!(queue[0].id, verifier_id);

    // Downstream stays blocked while the verdict is pending.
    let down = db.get_task(DOWN_ID).unwrap().unwrap();
    assert_eq!(down.state, TaskState::Blocked);
}

#[tokio::test]
async fn passing_verdict_completes_task_and_schedules_downstream() {
    let (db, bus) = setup().await;
    seed_agent(&db, WORKER_ID, AgentState::Active);
    seed_verified_scroll(&db, None);

    let verifier_id = drive_to_verification(&db, &bus).await;
    let verdict = serde_json::json!({
        "score": 0.95,
        "verdict": "pass",
        "rationale": "build succeeded"
    })
    .to_string();
    finish_verifier(&db, &bus, &verifier_id, &verdict).await;

    let task = db.get_task(TASK_ID).unwrap().unwrap();
    assert_eq!(task.state, TaskState::Complete);

    // The verdict is recorded against the worker for `grim eval --list`.
    let evals = db.list_eval_results(WORKER_ID).unwrap();
    assert_eq!(evals.len(), 1);
    assert!((evals[0].score - 0.95).abs() < 1e-9);
    assert_eq!(evals[0].verdict.as_deref(), Some("pass"));
    assert_eq!(evals[0].evaluator_id, verifier_id);

    // Downstream unblocked and spawned.
    let down = db.get_task(DOWN_ID).unwrap().unwrap();
    assert_eq!(down.state, TaskState::Active);
    assert!(down.agent_id.is_some());
}

#[tokio::test]
async fn failing_verdict_fails_task_and_skips_downstream() {
    let (db, bus) = setup().await;
    seed_agent(&db, WORKER_ID, AgentState::Active);
    seed_verified_scroll(&db, Some(0.5));

    let verifier_id = drive_to_verification(&db, &bus).await;
    let verdict = serde_json::json!({
        "score": 0.2,
        "verdict": "fail",
        "rationale": "tests are red"
    })
    .to_string();
    finish_verifier(&db, &bus, &verifier_id, &verdict).await;

    let task = db.get_task(TASK_ID).unwrap().unwrap();
    assert_eq!(task.state, TaskState::Failed);
    let down = db.get_task(DOWN_ID).unwrap().unwrap();
    assert_eq!(down.state, TaskState::Skipped);

    // Failing verdicts are still recorded.
    let evals = db.list_eval_results(WORKER_ID).unwrap();
    assert_eq!(evals.len(), 1);
    assert!((evals[0].score - 0.2).abs() < 1e-9);

    // Everything is terminal, so the scroll fails.
    let scroll = db.get_scroll(SCROLL_ID).unwrap().unwrap();
    assert_eq!(scroll.state, ScrollState::Failed);
}

#[tokio::test]
async fn garbage_verdict_fails_task_instead_of_hanging() {
    let (db, bus) = setup().await;
    seed_agent(&db, WORKER_ID, AgentState::Active);
    seed_verified_scroll(&db, None);

    let verifier_id = drive_to_verification(&db, &bus).await;
    finish_verifier(&db, &bus, &verifier_id, "no structured verdict here").await;

    let task = db.get_task(TASK_ID).unwrap().unwrap();
    assert_eq!(task.state, TaskState::Failed);
    let down = db.get_task(DOWN_ID).unwrap().unwrap();
    assert_eq!(down.state, TaskState::Skipped);

    // Nothing parseable, nothing recorded.
    assert!(db.list_eval_results(WORKER_ID).unwrap().is_empty());
}

#[tokio::test]
async fn verifier_agent_failure_fails_verification() {
    let (db, bus) = setup().await;
    seed_agent(&db, WORKER_ID, AgentState::Active);
    seed_verified_scroll(&db, None);

    let verifier_id = drive_to_verification(&db, &bus).await;
    bus.publish(StreamEvent::StateChange {
        agent_id: verifier_id,
        old_state: AgentState::Active,
        new_state: AgentState::Failed,
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let task = db.get_task(TASK_ID).unwrap().unwrap();
    assert_eq!(task.state, TaskState::Failed);
    let down = db.get_task(DOWN_ID).unwrap().unwrap();
    assert_eq!(down.state, TaskState::Skipped);
}
