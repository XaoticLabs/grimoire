// Tests for `monitor_agent`, which splits source-of-lines from
// persistence + publish.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::process_manager::{
    CapturedSessionId, LineEvent, LineSource, consume_lines, persist_event, publish_output,
};
use grimoire::daemon::provider::Provider;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{Agent, AgentState};

fn fresh_db() -> Arc<Database> {
    Arc::new(Database::open_in_memory().unwrap())
}

fn fresh_bus(db: Arc<Database>) -> EventBus {
    EventBus::new(db)
}

fn seed_agent(db: &Database, agent_id: &str) {
    let now = chrono::Utc::now();
    db.insert_agent(&Agent {
        id: agent_id.to_string(),
        name: None,
        state: AgentState::Active,
        task: Some("test".to_string()),
        model: None,
        provider: Some("noop".to_string()),
        cwd: std::path::PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: now,
        updated_at: now,
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    })
    .unwrap();
}

fn line(source: LineSource, line: &str) -> LineEvent {
    LineEvent {
        source,
        line: line.to_string(),
    }
}

struct NoopProvider;
impl Provider for NoopProvider {
    fn name(&self) -> &'static str {
        "noop"
    }
    fn capabilities(&self) -> grimoire::daemon::provider::ProviderCapabilities {
        unimplemented!()
    }
    fn spawn(
        &self,
        _: &str,
        _: &std::path::Path,
        _: Option<&str>,
        _: &grimoire::daemon::provider::AgentContext,
    ) -> Result<grimoire::daemon::process_manager::SpawnedAgent> {
        unimplemented!()
    }
    fn spawn_resume(
        &self,
        _: &str,
        _: &str,
        _: &std::path::Path,
        _: &grimoire::daemon::provider::AgentContext,
    ) -> Result<grimoire::daemon::process_manager::SpawnedAgent> {
        unimplemented!()
    }
    fn extract_session_id(&self, _: &str) -> Option<String> {
        None
    }
    fn extract_result(&self, _: &[String]) -> Option<String> {
        None
    }
}

#[tokio::test]
async fn consume_lines_persists_each_line_as_event() {
    let db = fresh_db();
    let bus = fresh_bus(db.clone());
    let agent_id = "agent-001".to_string();
    seed_agent(&db, &agent_id);

    let (tx, rx) = mpsc::channel::<LineEvent>(16);
    let stream = ReceiverStream::new(rx);

    for i in 0..5 {
        tx.send(line(LineSource::Stdout, &format!("line {i}")))
            .await
            .unwrap();
    }
    drop(tx);

    let _captured: CapturedSessionId =
        consume_lines(agent_id.clone(), stream, bus.clone(), db.clone(), None)
            .await
            .session_id;

    let events = db.get_events(&agent_id, None).unwrap();
    assert_eq!(events.len(), 5, "expected one DB row per line");
    for (i, ev) in events.iter().enumerate() {
        assert_eq!(ev.event_type, "stdout");
        assert_eq!(ev.payload, format!("line {i}"));
    }
}

#[tokio::test]
async fn consume_lines_publishes_streamevent_output_per_line() {
    let db = fresh_db();
    let bus = fresh_bus(db.clone());
    let mut rx = bus.subscribe();
    let agent_id = "agent-002".to_string();
    seed_agent(&db, &agent_id);

    let (tx, source_rx) = mpsc::channel::<LineEvent>(16);
    let stream = ReceiverStream::new(source_rx);

    let lines = [
        (LineSource::Stdout, "out-1"),
        (LineSource::Stderr, "err-1"),
        (LineSource::Stdout, "out-2"),
        (LineSource::Stderr, "err-2"),
        (LineSource::Stdout, "out-3"),
    ];
    for (s, l) in &lines {
        tx.send(line(*s, l)).await.unwrap();
    }
    drop(tx);

    let _ = consume_lines(agent_id.clone(), stream, bus.clone(), db.clone(), None).await;

    let mut received: Vec<(String, String)> = Vec::new();
    while let Ok(event) =
        tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await
    {
        match event {
            Ok(StreamEvent::Output { stream, line, .. }) => received.push((stream, line)),
            _ => break,
        }
    }
    assert_eq!(received.len(), 5);
    let stdout_count = received.iter().filter(|(s, _)| s == "stdout").count();
    let stderr_count = received.iter().filter(|(s, _)| s == "stderr").count();
    assert_eq!(stdout_count, 3);
    assert_eq!(stderr_count, 2);
}

#[tokio::test]
async fn consume_lines_captures_session_id_from_provider() {
    struct FakeProvider;
    impl Provider for FakeProvider {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn capabilities(&self) -> grimoire::daemon::provider::ProviderCapabilities {
            unimplemented!()
        }
        fn spawn(
            &self,
            _t: &str,
            _c: &std::path::Path,
            _m: Option<&str>,
            _ctx: &grimoire::daemon::provider::AgentContext,
        ) -> Result<grimoire::daemon::process_manager::SpawnedAgent> {
            unimplemented!()
        }
        fn spawn_resume(
            &self,
            _s: &str,
            _m: &str,
            _c: &std::path::Path,
            _ctx: &grimoire::daemon::provider::AgentContext,
        ) -> Result<grimoire::daemon::process_manager::SpawnedAgent> {
            unimplemented!()
        }
        fn extract_session_id(&self, line: &str) -> Option<String> {
            line.strip_prefix("SID=")
                .map(std::string::ToString::to_string)
        }
        fn extract_result(&self, _: &[String]) -> Option<String> {
            None
        }
    }

    let db = fresh_db();
    let bus = fresh_bus(db.clone());
    let provider: Arc<dyn Provider> = Arc::new(FakeProvider);
    let agent_id = "agent-003".to_string();
    seed_agent(&db, &agent_id);

    let (tx, rx) = mpsc::channel::<LineEvent>(8);
    let stream = ReceiverStream::new(rx);

    tx.send(line(LineSource::Stdout, "intro line"))
        .await
        .unwrap();
    tx.send(line(LineSource::Stdout, "SID=session-xyz"))
        .await
        .unwrap();
    tx.send(line(LineSource::Stdout, "after")).await.unwrap();
    drop(tx);

    let captured = consume_lines(agent_id, stream, bus, db, Some(provider))
        .await
        .session_id;
    assert_eq!(
        captured,
        Some("session-xyz".to_string()),
        "provider's extractor should set the session id"
    );
}

#[tokio::test]
async fn persist_event_and_publish_output_are_callable_from_outside_consume_lines() {
    // public-in-crate so RemoteExecutor writes events with the local shape
    let db = fresh_db();
    let bus = fresh_bus(db.clone());
    let mut rx = bus.subscribe();
    let agent_id = "agent-004".to_string();
    seed_agent(&db, &agent_id);

    persist_event(&db, &agent_id, LineSource::Stdout, "hello").unwrap();
    publish_output(&bus, &agent_id, LineSource::Stderr, "boom");

    let events = db.get_events(&agent_id, None).unwrap();
    assert_eq!(events.len(), 1, "persist_event writes exactly one row");
    assert_eq!(events[0].event_type, "stdout");
    assert_eq!(events[0].payload, "hello");

    match rx.try_recv() {
        Ok(StreamEvent::Output { stream, line, .. }) => {
            assert_eq!(stream, "stderr");
            assert_eq!(line, "boom");
        }
        other => panic!("expected StreamEvent::Output, got {other:?}"),
    }
}

#[tokio::test]
async fn monitor_agent_local_path_matches_pre_refactor_fixture() {
    // run the real monitor_agent against `printf` to pin the durable event shape
    use tokio::process::Command;

    let db = fresh_db();
    let bus = fresh_bus(db.clone());
    let agent_id = "agent-005".to_string();
    seed_agent(&db, &agent_id);

    let mut cmd = Command::new("/usr/bin/env");
    cmd.args(["bash", "-c", "printf 'a\\nb\\nc\\n'"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = cmd.spawn().unwrap();

    let provider: Arc<dyn Provider> = Arc::new(NoopProvider);
    let result = grimoire::daemon::process_manager::monitor_agent(
        agent_id.clone(),
        child,
        bus,
        db.clone(),
        Some(provider),
    )
    .await;

    assert_eq!(result.exit_code, Some(0));
    let events = db.get_events(&agent_id, None).unwrap();
    let stdout_lines: Vec<&str> = events
        .iter()
        .filter(|e| e.event_type == "stdout")
        .map(|e| e.payload.as_str())
        .collect();
    assert_eq!(stdout_lines, vec!["a", "b", "c"]);
}
