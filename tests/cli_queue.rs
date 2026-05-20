// Tests for `grim queue` (Task 11 of durable-work-queue spec).
//
// Exercises the formatter on `QueueEntry` fixtures plus a direct RPC-handler
// path that reads from a real in-memory DB. End-to-end CLI invocation is
// covered by the existing client; this file is the contract layer.

use std::sync::Arc;

use chrono::Utc;

use grimoire::cli::commands::queue as queue_cmd; // ensure the module compiles
use grimoire::cli::formatters;
use grimoire::daemon::persistence::{Database, QueueRow};
use grimoire::shared::protocol::{QueueEntry, QueueListResponse};
use grimoire::shared::types::{Agent, AgentState};
use std::path::PathBuf;

fn _ensure_module_loads() {
    // Touch the symbol so an unused-import warning doesn't fire on the
    // CI compile path; `queue_cmd::run` is async and not called here.
    let _ = &queue_cmd::run;
}

fn make_agent(id: &str) -> Agent {
    Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Queued,
        task: Some("t".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    }
}

fn make_row(id: &str, lane: &str, block: Option<&str>, t_offset: i64) -> QueueRow {
    QueueRow {
        id: id.to_string(),
        lane: lane.to_string(),
        priority: 0,
        enqueued_at: Utc::now() + chrono::Duration::seconds(t_offset),
        provider_name: Some("claude".into()),
        cwd: "/tmp".into(),
        model: None,
        task_text: "do thing".into(),
        block_reason: block.map(std::string::ToString::to_string),
    }
}

// --- Formatter contracts --------------------------------------------------

#[test]
fn cli_queue_text_format_has_columns() {
    let entries = vec![QueueEntry {
        id: "abc12345".into(),
        lane: "adhoc".into(),
        age_seconds: 12,
        provider: Some("claude".into()),
        cwd: "/tmp".into(),
        model: None,
        block_reason: Some("capacity".into()),
        task_text: "do thing".into(),
    }];
    let out = formatters::format_queue(&entries);
    assert!(out.contains("LANE"), "missing LANE header: {out}");
    assert!(out.contains("BLOCK"), "missing BLOCK header: {out}");
    assert!(out.contains("PROVIDER"), "missing PROVIDER header: {out}");
    assert!(out.contains("adhoc"));
    assert!(out.contains("capacity"));
}

#[test]
fn cli_queue_empty_prints_message() {
    let out = formatters::format_queue(&[]);
    assert!(
        out.to_lowercase().contains("no queued"),
        "empty queue should print no-queued message; got:\n{out}"
    );
}

#[test]
fn cli_queue_renders_no_worker_block_reason() {
    let entries = vec![QueueEntry {
        id: "id1".into(),
        lane: "scroll".into(),
        age_seconds: 0,
        provider: Some("absent".into()),
        cwd: "/tmp".into(),
        model: None,
        block_reason: Some("no_eligible_worker".into()),
        task_text: "x".into(),
    }];
    let out = formatters::format_queue(&entries);
    assert!(
        out.contains("no worker"),
        "no_eligible_worker should render as `no worker`; got:\n{out}"
    );
}

// --- RPC-shape contract via direct DB read --------------------------------
//
// The RPC handler shape is `db.list_queue() -> entries`. We exercise the
// transformation by inserting rows and verifying that the resulting
// `QueueListResponse` shape is parseable and ordered.

#[test]
fn queue_list_returns_pending_entries() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    db.insert_agent(&make_agent("aaa11111")).unwrap();
    db.insert_agent(&make_agent("bbb22222")).unwrap();
    db.enqueue_task(&make_row("aaa11111", "adhoc", None, 0))
        .unwrap();
    db.enqueue_task(&make_row("bbb22222", "scroll", Some("capacity"), 1))
        .unwrap();

    let rows = db.list_queue().unwrap();
    let entries: Vec<QueueEntry> = rows
        .into_iter()
        .map(|row| {
            let age = (Utc::now() - row.enqueued_at).num_seconds().max(0) as u64;
            QueueEntry {
                id: row.id,
                lane: row.lane,
                age_seconds: age,
                provider: row.provider_name,
                cwd: row.cwd,
                model: row.model,
                block_reason: row.block_reason,
                task_text: row.task_text,
            }
        })
        .collect();
    let resp = QueueListResponse { entries };

    assert_eq!(resp.entries.len(), 2);
    assert_eq!(resp.entries[0].lane, "adhoc", "ad-hoc lane drains first");
    assert_eq!(resp.entries[1].block_reason.as_deref(), Some("capacity"));
}

#[test]
fn cli_queue_json_emits_parseable_response() {
    let resp = QueueListResponse {
        entries: vec![QueueEntry {
            id: "abc".into(),
            lane: "adhoc".into(),
            age_seconds: 0,
            provider: None,
            cwd: "/tmp".into(),
            model: None,
            block_reason: None,
            task_text: "x".into(),
        }],
    };
    let json = serde_json::to_string(&resp).unwrap();
    let parsed: QueueListResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.entries.len(), 1);
    assert_eq!(parsed.entries[0].id, "abc");
}
