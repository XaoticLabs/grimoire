// Tests for worker.proto + tonic build wiring.

use prost::Message;

#[allow(unused_imports)]
use grimoire::shared::worker_proto::assign_task::OptionalResumeSessionId;
use grimoire::shared::worker_proto::{
    AssignTask, CancelTask, DaemonMessage, Heartbeat, ProviderCap, Register, TaskAccepted,
    TaskEvent, TaskFinished, TaskRejected, TaskState, WorkerMessage, assign_task::OptionalModel,
    daemon_message, task_event::EventKind, worker_control_client::WorkerControlClient,
    worker_control_server::WorkerControlServer, worker_message,
};

#[test]
fn worker_proto_register_message_roundtrip() {
    let original = Register {
        worker_id: "w-1234".to_string(),
        bearer_token: "secret".to_string(),
        worker_version: "1.0.0".to_string(),
        max_concurrent: 4,
        providers: vec![ProviderCap {
            name: "claude".to_string(),
            version: "1.2.3".to_string(),
        }],
        tags: vec!["beefy".to_string()],
        protocol_version: grimoire::shared::constants::WORKER_PROTOCOL_VERSION,
    };

    let mut buf = Vec::new();
    original.encode(&mut buf).expect("encode");
    let decoded = Register::decode(&*buf).expect("decode");

    assert_eq!(decoded.worker_id, original.worker_id);
    assert_eq!(decoded.bearer_token, original.bearer_token);
    assert_eq!(decoded.worker_version, original.worker_version);
    assert_eq!(decoded.max_concurrent, original.max_concurrent);
    assert_eq!(decoded.providers.len(), 1);
    assert_eq!(decoded.providers[0].name, "claude");
    assert_eq!(decoded.providers[0].version, "1.2.3");
    assert_eq!(decoded.tags, vec!["beefy"]);
}

#[test]
fn worker_proto_assign_task_optional_fields_default_to_none() {
    let original = AssignTask {
        agent_id: "a-1".to_string(),
        task: "echo hi".to_string(),
        provider_constraint: ">=1.0".to_string(),
        provider_name: "claude".to_string(),
        cwd: "/tmp".to_string(),
        env: std::collections::HashMap::new(),
        // Both optional fields explicitly absent.
        model: None,
        optional_resume_session_id: None,
    };

    let mut buf = Vec::new();
    original.encode(&mut buf).unwrap();
    let decoded = AssignTask::decode(&*buf).unwrap();

    assert_eq!(decoded.model, None);
    assert_eq!(decoded.optional_resume_session_id, None);
    assert_eq!(decoded.task, "echo hi");
    assert!(decoded.env.is_empty());
}

const fn check_client_type<T>() {}
const fn check_server_type<T>() {}

#[test]
fn worker_proto_compiles() {
    // Type-only checks: each oneof variant exists; client and server stubs
    // are reachable from the public surface. If this file compiles, the
    // proto wiring satisfies the contract.
    let _: fn(_) -> WorkerMessage = |kind: worker_message::Kind| WorkerMessage { kind: Some(kind) };
    let _: fn(_) -> DaemonMessage = |kind: daemon_message::Kind| DaemonMessage { kind: Some(kind) };

    let _ = worker_message::Kind::Register(Register::default());
    let _ = worker_message::Kind::Heartbeat(Heartbeat::default());
    let _ = worker_message::Kind::TaskAccepted(TaskAccepted::default());
    let _ = worker_message::Kind::TaskRejected(TaskRejected::default());
    let _ = worker_message::Kind::TaskEvent(TaskEvent::default());
    let _ = worker_message::Kind::TaskFinished(TaskFinished::default());

    let _ = daemon_message::Kind::AssignTask(AssignTask::default());
    let _ = daemon_message::Kind::CancelTask(CancelTask::default());

    let _ = EventKind::Stdout;
    let _ = EventKind::Stderr;
    let _ = TaskState::Complete;
    let _ = TaskState::Failed;
    let _ = TaskState::Banished;

    // Reference the generated client/server stubs so the symbol is exercised.
    check_client_type::<WorkerControlClient<tonic::transport::Channel>>();
    check_server_type::<WorkerControlServer<()>>();

    // OptionalModel was introduced as a sanity check that prost's `optional`
    // field markers produce real `Option<T>` types (no oneof wrapper).
    let _: Option<OptionalModel> = None;
}
