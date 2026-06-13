//! Live two-daemon federation tests: two in-process mTLS daemons exercise
//! workspace-event, remote-file-watch, remote-agent-completion, and
//! scroll-dispatch flows over real gRPC. File events are simulated at the
//! outbox level (no notify watcher); everything past the enqueue is real wire traffic.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tonic::transport::{Certificate, Server, ServerTlsConfig};

use grimoire::daemon::agent_lifecycle_publisher;
use grimoire::daemon::clock::SystemClock;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::peer_client::ScrollDispatchPayload;
use grimoire::daemon::peer_registry::PeerRegistry;
use grimoire::daemon::peer_rpc_server::PeerSvc;
use grimoire::daemon::persistence::{Database, unix_now};
use grimoire::daemon::wake_registry::WakeRegistry;
use grimoire::daemon::wake_sources::remote_agent_completion::RemoteAgentCompletionConfig;
use grimoire::daemon::wake_sources::remote_file_watch::RemoteFileWatchConfig;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::tls::Identity;
use grimoire::shared::types::{
    Agent, AgentState, FederationDirection, Peer, PeerState, RestartPolicy, Subscription,
    Workspace, WorkspaceKind, WorkspaceState,
};

/// Shared by both link directions: each side's row for the other carries the
/// same token, so the receiver's token-hash lookup resolves regardless of dialer.
const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";

/// Deadline for cross-daemon observations; poll loops return early once satisfied.
const WAIT: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(25);

struct TestDaemon {
    daemon_id: String,
    registry: Arc<PeerRegistry>,
    identity: Arc<Identity>,
    db: Arc<Database>,
    bus: EventBus,
    addr: std::net::SocketAddr,
}

impl TestDaemon {
    fn url(&self) -> String {
        format!("https://{}", self.addr)
    }
}

/// Boot one in-process daemon with an mTLS peer listener presenting `identity`
/// and trusting `trusted_client_certs` as the client-CA bundle.
async fn boot_daemon(
    daemon_id: &str,
    identity: Identity,
    trusted_client_certs: &[String],
) -> TestDaemon {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.db");
    std::mem::forget(dir);
    let db = Arc::new(Database::open(&path).unwrap());
    let bus = EventBus::new(db.clone());
    let clock = Arc::new(SystemClock);
    let identity = Arc::new(identity);
    let registry = PeerRegistry::new(
        db.clone(),
        bus.clone(),
        clock,
        daemon_id.to_string(),
        identity.clone(),
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mut tls = ServerTlsConfig::new().identity(identity.to_tonic());
    if !trusted_client_certs.is_empty() {
        tls = tls.client_ca_root(Certificate::from_pem(
            trusted_client_certs.join("\n").as_bytes(),
        ));
    }
    let svc = PeerSvc::new(registry.clone());
    tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(svc)
            .serve(addr)
            .await;
    });
    // Warm-up only; the client retries with backoff, so this is not load-bearing.
    tokio::time::sleep(Duration::from_millis(100)).await;

    TestDaemon {
        daemon_id: daemon_id.to_string(),
        registry,
        identity,
        db,
        bus,
        addr,
    }
}

/// Boot a mutually-trusting (A, B) pair: each daemon's listener pins the
/// other's client cert, so either side can dial.
async fn boot_pair() -> (TestDaemon, TestDaemon) {
    let id_a = grimoire::shared::tls::generate("daemon").unwrap();
    let id_b = grimoire::shared::tls::generate("daemon").unwrap();
    let a = boot_daemon("aaaaaaaa", id_a.clone(), &[id_b.cert_pem().to_string()]).await;
    let b = boot_daemon("bbbbbbbb", id_b, &[id_a.cert_pem().to_string()]).await;
    (a, b)
}

/// Seed `on`'s peer row for `from`: token hash for handshake auth, plus `from`'s
/// url + pinned cert so `on` can also dial out over the same row (reverse link).
fn seed_peer_row(on: &TestDaemon, id: &str, from: &TestDaemon, daemon_id: &str) {
    let token_hash = blake3::hash(TOKEN.as_bytes()).as_bytes().to_vec();
    let peer = Peer {
        id: id.to_string(),
        daemon_id: daemon_id.to_string(),
        name: format!("link-{id}"),
        url: from.url(),
        bearer_token_hash: token_hash,
        bearer_token: TOKEN.to_string(),
        public_key: Some(from.identity.cert_pem().as_bytes().to_vec()),
        state: PeerState::Pending,
        last_seen: None,
        registered_at: unix_now(),
    };
    on.db.insert_peer(&peer).unwrap();
}

/// Establish the A -> B link: seed B's row for A, register B as a peer on A,
/// and wait for the handshake. Returns A's peer row for B (daemon_id resolved).
async fn link_a_to_b(a: &TestDaemon, b: &TestDaemon, a_on_b_id: &str) -> Peer {
    seed_peer_row(b, a_on_b_id, a, &a.daemon_id);
    let peer_b = a
        .registry
        .register_peer("b", &b.url(), TOKEN, b.identity.cert_pem(), 10)
        .await
        .expect("A -> B handshake should succeed");
    assert_eq!(peer_b.daemon_id, b.daemon_id);
    peer_b
}

/// Poll `f` until it yields a value or the deadline elapses.
async fn wait_for<T, F: FnMut() -> Option<T>>(what: &str, mut f: F) -> T {
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Drain a bus subscription until `pred` extracts a value. Lag is tolerated; a
/// dropped awaited event surfaces as a deadline failure.
async fn expect_event<T, F: FnMut(&StreamEvent) -> Option<T>>(
    rx: &mut tokio::sync::broadcast::Receiver<StreamEvent>,
    what: &str,
    mut pred: F,
) -> T {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let now = tokio::time::Instant::now();
        assert!(now < deadline, "timed out waiting for {what}");
        match tokio::time::timeout(deadline - now, rx.recv()).await {
            Ok(Ok(ev)) => {
                if let Some(v) = pred(&ev) {
                    return v;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(e)) => panic!("bus closed while waiting for {what}: {e}"),
            Err(elapsed) => panic!("timed out ({elapsed}) waiting for {what}"),
        }
    }
}

fn seed_agent(db: &Database, id: &str, state: AgentState) {
    let a = Agent {
        id: id.to_string(),
        name: None,
        state,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: Some("sess".into()),
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&a).unwrap();
}

/// Producer side of a federated workspace on A + shadow side on B.
/// Returns the shadow workspace id on B.
fn federate_workspace(a: &TestDaemon, peer_b_id: &str, b: &TestDaemon, a_on_b_id: &str) -> String {
    let ws = Workspace {
        id: "wsA".into(),
        path: PathBuf::from("/tmp/grimoire-test-wsA"),
        repo_path: PathBuf::from("/tmp/grimoire-test-repo"),
        branch: "main".into(),
        state: WorkspaceState::Active,
        created_at: Utc::now(),
        kind: WorkspaceKind::Local,
        home_daemon_id: None,
        home_workspace_id: None,
    };
    a.db.insert_workspace(&ws).unwrap();
    a.db.upsert_workspace_federation(
        "wf-a",
        peer_b_id,
        "wsA",
        FederationDirection::Outbound,
        unix_now(),
    )
    .unwrap();

    let shadow_id = "shadow1".to_string();
    b.db.insert_shadow_workspace(&shadow_id, &a.daemon_id, "wsA", "main", Utc::now())
        .unwrap();
    b.db.upsert_workspace_federation(
        "wf-b",
        a_on_b_id,
        &shadow_id,
        FederationDirection::Inbound,
        unix_now(),
    )
    .unwrap();
    shadow_id
}

/// Simulate a `WorkspaceWatcher` batch on `d`: one outbox row per outbound peer
/// plus a drainer wake — the steps `workspace_watcher::fanout_to_federated_peers`
/// runs after a notify event, minus the real filesystem watcher.
async fn fanout_workspace_event(
    d: &TestDaemon,
    workspace_id: &str,
    paths: &[&str],
    kinds: &[&str],
) {
    let payload = serde_json::json!({
        "paths": paths,
        "kinds": kinds,
        "truncated": 0,
    })
    .to_string();
    let peers = d.db.workspace_outbound_peers(workspace_id).unwrap();
    assert!(!peers.is_empty(), "expected at least one outbound peer");
    for pid in peers {
        d.db.workspace_event_enqueue(&pid, workspace_id, payload.as_bytes())
            .unwrap();
        d.registry.notify_outbox(&pid).await;
    }
}

/// A file-change batch on A's federated workspace crosses the wire and is
/// republished on B's shadow workspace as both a `WorkspaceFileChanged` bus
/// event and `workspace/<id>/files` topic mail to local subscribers.
#[tokio::test]
async fn workspace_file_event_federates_to_shadow_workspace() {
    let (a, b) = boot_pair().await;
    let peer_b = link_a_to_b(&a, &b, "a-on-b").await;
    let shadow_id = federate_workspace(&a, &peer_b.id, &b, "a-on-b");

    // Local subscriber on B's shadow files topic.
    seed_agent(&b.db, "subB1", AgentState::Dormant);
    b.db.insert_subscription(&Subscription {
        id: "sub1".into(),
        subscriber_id: "subB1".into(),
        topic: format!("workspace/{shadow_id}/files"),
        created_at: unix_now(),
    })
    .unwrap();

    let mut rx_b = b.bus.subscribe();
    fanout_workspace_event(
        &a,
        "wsA",
        &["src/main.rs", "README.md"],
        &["modify", "create"],
    )
    .await;

    // Republish carries B's local workspace id and A's paths.
    let paths = expect_event(&mut rx_b, "WorkspaceFileChanged on B's shadow", |ev| {
        if let StreamEvent::WorkspaceFileChanged {
            workspace_id,
            paths,
            ..
        } = ev
        {
            (*workspace_id == shadow_id).then(|| paths.clone())
        } else {
            None
        }
    })
    .await;
    assert_eq!(paths, vec!["src/main.rs".to_string(), "README.md".into()]);

    let mail = wait_for("topic mail for subB1", || {
        b.db.list_pending_wake_eligible("subB1")
            .unwrap()
            .into_iter()
            .next()
    })
    .await;
    assert_eq!(mail.topic.as_deref(), Some("workspace/shadow1/files"));
    assert!(
        mail.body.contains("src/main.rs"),
        "topic mail should carry the federated paths: {}",
        mail.body
    );

    // The ack deletes A's delivered outbox row.
    wait_for("A's workspace outbox to drain", || {
        let pending =
            a.db.workspace_event_next_outbox(&peer_b.id, unix_now() + 120)
                .unwrap();
        pending.is_none().then_some(())
    })
    .await;
}

/// An agent on B with a `remote_file_watch` wake source against the shadow
/// workspace wakes when A's federated file event arrives. Ignore globs are honored.
#[tokio::test]
async fn remote_file_watch_wake_fires_across_daemons() {
    let (a, b) = boot_pair().await;
    let peer_b = link_a_to_b(&a, &b, "a-on-b").await;
    let shadow_id = federate_workspace(&a, &peer_b.id, &b, "a-on-b");

    seed_agent(&b.db, "watcherB", AgentState::Dormant);
    let wake_reg =
        WakeRegistry::with_default_sender(b.db.clone(), b.bus.clone(), Arc::new(SystemClock));
    wake_reg.spawn();
    wake_reg
        .register_remote_file_watch(
            "watcherB",
            RemoteFileWatchConfig {
                workspace_id: shadow_id,
                globs: vec!["src/**/*.rs".into()],
                ignore: vec!["target/**".into()],
            },
        )
        .await
        .unwrap();

    fanout_workspace_event(
        &a,
        "wsA",
        &["src/lib.rs", "target/debug/junk.rs"],
        &["modify", "create"],
    )
    .await;

    let mail = wait_for("wake mail for watcherB", || {
        b.db.list_pending_wake_eligible("watcherB")
            .unwrap()
            .into_iter()
            .find(|m| {
                m.sender_id
                    .as_deref()
                    .is_some_and(|s| s.starts_with("wake://"))
            })
    })
    .await;
    assert!(
        mail.body.contains("[remote-file-watch]"),
        "unexpected wake body: {}",
        mail.body
    );
    assert!(
        mail.body.contains("src/lib.rs"),
        "first matched path should be the non-ignored one: {}",
        mail.body
    );
    assert!(
        mail.body.contains("1 matches"),
        "ignored path must not count as a match: {}",
        mail.body
    );
}

/// Agent lifecycle on A federates to B; an agent on B with a
/// `remote_agent_completion` wake source on A's parent wakes when the parent
/// completes — the wire path behind `grim summon --on-remote-parent`.
#[tokio::test]
async fn remote_agent_completion_wake_fires_across_daemons() {
    let (a, b) = boot_pair().await;
    let peer_b = link_a_to_b(&a, &b, "a-on-b").await;

    // Lifecycle federation: A outbound to B; B authorizes inbound from A.
    a.db.upsert_agent_lifecycle_federation(
        "alf-a",
        &peer_b.id,
        FederationDirection::Outbound,
        unix_now(),
    )
    .unwrap();
    b.db.upsert_agent_lifecycle_federation(
        "alf-b",
        "a-on-b",
        FederationDirection::Inbound,
        unix_now(),
    )
    .unwrap();
    // Producer: fans local StateChange events into the lifecycle outbox.
    agent_lifecycle_publisher::spawn(a.db.clone(), a.bus.clone(), a.registry.clone());

    // Remote parent on A; waiting child on B.
    seed_agent(&a.db, "parentA1", AgentState::Active);
    seed_agent(&b.db, "childB1", AgentState::Dormant);
    let wake_reg =
        WakeRegistry::with_default_sender(b.db.clone(), b.bus.clone(), Arc::new(SystemClock));
    wake_reg.spawn();
    wake_reg
        .register_remote_agent_completion(
            "childB1",
            RemoteAgentCompletionConfig {
                sender_daemon_id: a.daemon_id.clone(),
                remote_agent_id: "parentA1".into(),
                states: vec![],
            },
        )
        .await
        .unwrap();

    let mut rx_b = b.bus.subscribe();

    // The remote parent completes on A.
    a.bus.publish(StreamEvent::StateChange {
        agent_id: "parentA1".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });

    // B republishes the federated transition, then fires the wake mail.
    expect_event(&mut rx_b, "RemoteAgentStateChanged on B", |ev| {
        if let StreamEvent::RemoteAgentStateChanged {
            sender_daemon_id,
            agent_id,
            new_state,
            ..
        } = ev
        {
            (*sender_daemon_id == a.daemon_id
                && agent_id == "parentA1"
                && *new_state == AgentState::Complete)
                .then_some(())
        } else {
            None
        }
    })
    .await;

    let mail = wait_for("wake mail for childB1", || {
        b.db.list_pending_wake_eligible("childB1")
            .unwrap()
            .into_iter()
            .find(|m| {
                m.sender_id
                    .as_deref()
                    .is_some_and(|s| s.starts_with("wake://"))
            })
    })
    .await;
    assert!(
        mail.body
            .contains("[remote-parent parentA1@aaaaaaaa -> complete]"),
        "unexpected wake body: {}",
        mail.body
    );
}

/// A dispatches a scroll task to B; B (opted in via `accept_scroll_dispatch`)
/// queues a local agent and acks with its id, patching A's dispatch row. B's
/// completion federates back as a `RemoteAgentStateChanged`.
///
/// Covers outbox -> wire -> receiver agent+queue rows -> ack -> row patch ->
/// lifecycle return leg. Not covered: running B's queued agent (needs a
/// scheduler) and A's ScrollKeeper DAG bookkeeping (unit-tested in scroll_keeper).
#[tokio::test]
async fn scroll_dispatch_round_trip_with_lifecycle_return() {
    let (a, b) = boot_pair().await;
    let peer_b = link_a_to_b(&a, &b, "a-on-b").await;

    // B opts in to running dispatched scroll tasks from A.
    b.db.set_peer_accept_scroll_dispatch("a-on-b", true)
        .unwrap();

    // Reverse link B -> A over B's seeded row, so B's lifecycle outbox can drain to A.
    let a_on_b = b.db.get_peer("a-on-b").unwrap().unwrap();
    b.registry.ensure_connected(&a_on_b).await.unwrap();

    // Lifecycle federation for the return leg: B outbound, A inbound.
    b.db.upsert_agent_lifecycle_federation(
        "alf-b",
        "a-on-b",
        FederationDirection::Outbound,
        unix_now(),
    )
    .unwrap();
    a.db.upsert_agent_lifecycle_federation(
        "alf-a",
        &peer_b.id,
        FederationDirection::Inbound,
        unix_now(),
    )
    .unwrap();
    agent_lifecycle_publisher::spawn(b.db.clone(), b.bus.clone(), b.registry.clone());

    let mut rx_a = a.bus.subscribe();

    // Coordinator side: durable dispatch row + wire outbox row, as
    // `handle_scroll_dispatch_task` does after task lookup.
    let payload = ScrollDispatchPayload {
        scroll_id: "scr1".into(),
        task_id: "task1".into(),
        task_name: "build".into(),
        prompt: "do the thing".into(),
        provider: String::new(),
        model: String::new(),
        cwd: String::new(),
        file_patterns: vec![],
    };
    a.db.scroll_dispatch_insert("disp1", "scr1", "task1", &peer_b.id)
        .unwrap();
    let seq =
        a.db.scroll_dispatch_enqueue(&peer_b.id, &serde_json::to_vec(&payload).unwrap())
            .unwrap();
    a.registry.notify_outbox(&peer_b.id).await;

    // B records the inbound dispatch and queues a local agent for it.
    let local_agent_id = wait_for("B to record the inbound dispatch", || {
        b.db.scroll_dispatch_inbox_lookup(&a.daemon_id, seq)
            .unwrap()
    })
    .await;
    let agent = b.db.get_agent(&local_agent_id).unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Queued);
    assert_eq!(agent.name.as_deref(), Some("dispatched:build"));
    assert_eq!(agent.task.as_deref(), Some("do the thing"));
    assert!(
        b.db.list_queue()
            .unwrap()
            .iter()
            .any(|q| q.id == local_agent_id && q.lane == "default"),
        "dispatched agent should sit in B's default queue lane"
    );

    // The ack patched A's dispatch row with B's local agent id.
    let dispatch = wait_for("A's dispatch row to be patched by the ack", || {
        a.db.scroll_dispatch_find_by_remote(&peer_b.id, &local_agent_id)
            .unwrap()
    })
    .await;
    assert_eq!(dispatch.scroll_id, "scr1");
    assert_eq!(dispatch.task_id, "task1");
    assert_eq!(dispatch.state, "dispatched");
    assert_eq!(dispatch.remote_agent_id.as_deref(), Some(&*local_agent_id));

    // Return leg: B's dispatched agent completes; the lifecycle event
    // federates back and surfaces on A's bus, attributed to B.
    b.bus.publish(StreamEvent::StateChange {
        agent_id: local_agent_id.clone(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    expect_event(&mut rx_a, "RemoteAgentStateChanged on A", |ev| {
        if let StreamEvent::RemoteAgentStateChanged {
            sender_daemon_id,
            agent_id,
            new_state,
            name,
            ..
        } = ev
        {
            (*sender_daemon_id == b.daemon_id
                && *agent_id == local_agent_id
                && *new_state == AgentState::Complete
                && name.as_deref() == Some("dispatched:build"))
            .then_some(())
        } else {
            None
        }
    })
    .await;
}

/// Regression: a freshly added peer row has an empty `daemon_id` until
/// its own outbound handshake fills it. If the *inbound* handshake is
/// the first to arrive, the receiver must use the `Hello`'s daemon-id
/// for that session — otherwise every federated workspace event on the
/// first session resolves no shadow (empty home daemon-id) and is
/// silently dropped with a positive ack.
#[tokio::test]
async fn first_inbound_session_uses_handshake_daemon_id() {
    let (a, b) = boot_pair().await;
    // Seed B's row for A with an UNRESOLVED daemon_id, as `peer add` leaves it.
    seed_peer_row(&b, "a-on-b", &a, "");
    let peer_b = a
        .registry
        .register_peer("b", &b.url(), TOKEN, b.identity.cert_pem(), 10)
        .await
        .expect("A -> B handshake should succeed");

    let shadow_id = federate_workspace(&a, &peer_b.id, &b, "a-on-b");

    // The handshake persisted A's daemon-id onto B's row.
    let row = b.db.get_peer("a-on-b").unwrap().unwrap();
    assert_eq!(row.daemon_id, a.daemon_id);

    let mut rx_b = b.bus.subscribe();
    fanout_workspace_event(&a, "wsA", &["src/first.rs"], &["create"]).await;

    expect_event(
        &mut rx_b,
        "WorkspaceFileChanged on B during the first inbound session",
        |ev| {
            if let StreamEvent::WorkspaceFileChanged {
                workspace_id,
                paths,
                ..
            } = ev
            {
                (*workspace_id == shadow_id && paths == &["src/first.rs".to_string()]).then_some(())
            } else {
                None
            }
        },
    )
    .await;
}
