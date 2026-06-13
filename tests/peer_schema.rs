//! Contract tests for federation schema, dedupe, and cascade.

use grimoire::daemon::persistence::{Database, unix_now};
use grimoire::shared::types::{Peer, PeerState};
use std::sync::Arc;

fn fresh_db() -> Arc<Database> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("grimoire.db");
    std::mem::forget(dir); // leak tempdir so the db file outlives this fn
    Arc::new(Database::open(&path).unwrap())
}

fn fake_peer(name: &str, token: &str) -> Peer {
    let token_hash = blake3::hash(token.as_bytes()).as_bytes().to_vec();
    Peer {
        id: format!("p-{name}"),
        daemon_id: String::new(),
        name: name.to_string(),
        url: "http://127.0.0.1:1".to_string(),
        bearer_token_hash: token_hash,
        bearer_token: token.to_string(),
        public_key: None,
        state: PeerState::Pending,
        last_seen: None,
        registered_at: unix_now(),
    }
}

#[test]
fn migrate_creates_peer_tables() {
    let db = fresh_db();
    db.insert_peer(&fake_peer("alpha", "0123456789abcdef0123456789abcdef"))
        .unwrap();
    let p = db.get_peer_by_name("alpha").unwrap().unwrap();
    assert_eq!(p.name, "alpha");
    assert_eq!(p.state, PeerState::Pending);
}

#[test]
fn peer_inbox_pk_dedupes() {
    let db = fresh_db();
    let inserted_first = db
        .insert_peer_inbox_if_absent("aaaaaaaa", 1, "mail-1", unix_now())
        .unwrap();
    let inserted_again = db
        .insert_peer_inbox_if_absent("aaaaaaaa", 1, "mail-1", unix_now())
        .unwrap();
    assert!(inserted_first);
    assert!(!inserted_again);
}

#[test]
fn peers_cascade_deletes_outbox() {
    let db = fresh_db();
    let peer = fake_peer("beta", "0123456789abcdef0123456789abcdef");
    db.insert_peer(&peer).unwrap();
    let mail = grimoire::shared::types::Mail {
        id: "m1".into(),
        recipient_id: "agent://grimd-12345678/abcd1234".into(),
        sender_id: None,
        topic: None,
        body: "hi".into(),
        in_reply_to: None,
        state: grimoire::shared::types::MailState::Pending,
        fail_reason: None,
        created_at: unix_now(),
        delivered_at: None,
        seq: 0,
        wake_eligible: true,
    };
    db.insert_mail_with_outbox(
        &mail,
        &peer.id,
        "ob1",
        "agent://grimd-12345678/abcd1234",
        None,
        unix_now(),
    )
    .unwrap();
    assert_eq!(db.outbox_depth(&peer.id).unwrap(), 1);
    db.delete_peer(&peer.id).unwrap();
    assert_eq!(db.outbox_depth(&peer.id).unwrap(), 0);
}

#[test]
fn topic_federation_direction_merge_idempotent() {
    use grimoire::shared::types::FederationDirection;
    let db = fresh_db();
    let peer = fake_peer("gamma", "0123456789abcdef0123456789abcdef");
    db.insert_peer(&peer).unwrap();
    let dir1 = db
        .upsert_topic_federation(
            "f1",
            &peer.id,
            "pr-opened",
            FederationDirection::Outbound,
            unix_now(),
        )
        .unwrap();
    assert_eq!(dir1, FederationDirection::Outbound);
    let dir2 = db
        .upsert_topic_federation(
            "f2",
            &peer.id,
            "pr-opened",
            FederationDirection::Inbound,
            unix_now(),
        )
        .unwrap();
    assert_eq!(dir2, FederationDirection::Both);
}

/// UNIQUE(peer_id, workspace_id) means re-federate is an upsert, and
/// Outbound + Inbound merges to Both.
#[test]
fn workspace_federation_merges_and_lists() {
    use chrono::Utc;
    use grimoire::shared::types::FederationDirection;
    let db = fresh_db();
    let peer = fake_peer("delta", "abcdef0123456789abcdef0123456789");
    db.insert_peer(&peer).unwrap();

    let ws = grimoire::shared::types::Workspace {
        id: "frontend".into(),
        path: "/tmp/frontend".into(),
        repo_path: "/tmp/repo".into(),
        branch: "main".into(),
        state: grimoire::shared::types::WorkspaceState::Active,
        created_at: Utc::now(),
        kind: grimoire::shared::types::WorkspaceKind::Local,
        home_daemon_id: None,
        home_workspace_id: None,
    };
    db.insert_workspace(&ws).unwrap();

    let d1 = db
        .upsert_workspace_federation(
            "wf1",
            &peer.id,
            "frontend",
            FederationDirection::Outbound,
            unix_now(),
        )
        .unwrap();
    assert_eq!(d1, FederationDirection::Outbound);
    let d2 = db
        .upsert_workspace_federation(
            "wf2",
            &peer.id,
            "frontend",
            FederationDirection::Inbound,
            unix_now(),
        )
        .unwrap();
    assert_eq!(d2, FederationDirection::Both);

    let rows = db.list_workspace_federations_for("frontend").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].direction, FederationDirection::Both);

    let peers = db.workspace_outbound_peers("frontend").unwrap();
    assert_eq!(peers, vec![peer.id.clone()]);
    assert!(
        db.workspace_federation_inbound_authorized(&peer.id, "frontend")
            .unwrap()
    );

    // unfederate is idempotent
    assert_eq!(
        db.delete_workspace_federation(&peer.id, "frontend")
            .unwrap(),
        1
    );
    assert_eq!(
        db.delete_workspace_federation(&peer.id, "frontend")
            .unwrap(),
        0
    );
}

/// Shadow rows carry home ids and live at the `shadow://…` sentinel path,
/// which preserves UNIQUE(path) against the real local workspace.
#[test]
fn shadow_workspace_roundtrips() {
    use chrono::Utc;
    let db = fresh_db();
    db.insert_shadow_workspace(
        "frontend-shadow",
        "abcdef01",
        "frontend",
        "main",
        Utc::now(),
    )
    .unwrap();
    let got = db.get_workspace("frontend-shadow").unwrap().unwrap();
    assert_eq!(got.kind, grimoire::shared::types::WorkspaceKind::Shadow);
    assert_eq!(got.home_daemon_id.as_deref(), Some("abcdef01"));
    assert_eq!(got.home_workspace_id.as_deref(), Some("frontend"));
    assert_eq!(
        got.path.to_string_lossy(),
        "shadow://abcdef01/frontend",
        "shadow path uses sentinel scheme to preserve UNIQUE(path)"
    );
}

#[test]
fn find_shadow_workspace_resolves() {
    use chrono::Utc;
    let db = fresh_db();
    db.insert_shadow_workspace("frontend-shadow", "homeD", "frontend", "main", Utc::now())
        .unwrap();

    let hit = db
        .find_shadow_workspace("homeD", "frontend")
        .unwrap()
        .unwrap();
    assert_eq!(hit, "frontend-shadow");

    assert!(
        db.find_shadow_workspace("homeD", "backend")
            .unwrap()
            .is_none()
    );
    assert!(
        db.find_shadow_workspace("otherD", "frontend")
            .unwrap()
            .is_none()
    );
}

/// Scroll dispatch wire layer round-trips; inbox replay yields the stored
/// `local_agent_id` instead of spawning a duplicate.
#[test]
fn scroll_dispatch_schema_and_inbox() {
    let db = fresh_db();
    let peer = fake_peer("zeta", "0123456789abcdef0123456789abcdef");
    db.insert_peer(&peer).unwrap();

    assert!(!db.peer_accept_scroll_dispatch(&peer.id).unwrap());
    db.set_peer_accept_scroll_dispatch(&peer.id, true).unwrap();
    assert!(db.peer_accept_scroll_dispatch(&peer.id).unwrap());
    db.set_peer_accept_scroll_dispatch(&peer.id, false).unwrap();
    assert!(!db.peer_accept_scroll_dispatch(&peer.id).unwrap());

    db.scroll_dispatch_insert("disp1", "scr1", "task1", &peer.id)
        .unwrap();
    db.scroll_dispatch_set_remote_agent("scr1", "task1", &peer.id, "remote-agent-a")
        .unwrap();
    let row = db
        .scroll_dispatch_find_by_remote(&peer.id, "remote-agent-a")
        .unwrap()
        .unwrap();
    assert_eq!(row.task_id, "task1");
    assert_eq!(row.state, "dispatched");

    assert!(
        db.scroll_dispatch_inbox_lookup("homeA", 7)
            .unwrap()
            .is_none()
    );
    db.scroll_dispatch_inbox_record("homeA", 7, "local-agent-x")
        .unwrap();
    assert_eq!(
        db.scroll_dispatch_inbox_lookup("homeA", 7).unwrap(),
        Some("local-agent-x".to_string())
    );
    // replay record is a no-op (INSERT OR IGNORE)
    db.scroll_dispatch_inbox_record("homeA", 7, "ghost-agent")
        .unwrap();
    assert_eq!(
        db.scroll_dispatch_inbox_lookup("homeA", 7).unwrap(),
        Some("local-agent-x".to_string())
    );
}

#[test]
fn agent_lifecycle_federation_and_inbox() {
    use grimoire::shared::types::FederationDirection;
    let db = fresh_db();
    let peer = fake_peer("epsilon", "0123456789abcdef0123456789abcdef");
    db.insert_peer(&peer).unwrap();

    let d1 = db
        .upsert_agent_lifecycle_federation("alf1", &peer.id, FederationDirection::Outbound, 0)
        .unwrap();
    assert_eq!(d1, FederationDirection::Outbound);
    let d2 = db
        .upsert_agent_lifecycle_federation("alf2", &peer.id, FederationDirection::Inbound, 0)
        .unwrap();
    assert_eq!(d2, FederationDirection::Both);

    let peers = db.agent_lifecycle_outbound_peers().unwrap();
    assert_eq!(peers, vec![peer.id.clone()]);
    assert!(
        db.agent_lifecycle_inbound_authorized(&peer.id).unwrap(),
        "Both direction includes inbound"
    );

    assert!(db.agent_lifecycle_inbox_record("homeA", 1).unwrap());
    assert!(
        !db.agent_lifecycle_inbox_record("homeA", 1).unwrap(),
        "replay suppressed"
    );
    assert!(db.agent_lifecycle_inbox_record("homeA", 2).unwrap());
    assert!(
        db.agent_lifecycle_inbox_record("homeB", 1).unwrap(),
        "different sender is independent"
    );

    // unfederate is idempotent
    assert_eq!(db.delete_agent_lifecycle_federation(&peer.id).unwrap(), 1);
    assert_eq!(db.delete_agent_lifecycle_federation(&peer.id).unwrap(), 0);
}

#[test]
fn workspace_event_inbox_dedupes() {
    let db = fresh_db();

    assert!(
        db.workspace_event_inbox_record("homeA", 1, "shadow1")
            .unwrap()
    );
    assert!(
        !db.workspace_event_inbox_record("homeA", 1, "shadow1")
            .unwrap(),
        "replay of same (sender, seq) is suppressed"
    );
    assert!(
        db.workspace_event_inbox_record("homeA", 2, "shadow1")
            .unwrap(),
        "new seq from same sender is accepted"
    );
    assert!(
        db.workspace_event_inbox_record("homeB", 1, "shadow1")
            .unwrap(),
        "same seq from a different sender is independent"
    );
}
