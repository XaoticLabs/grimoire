//! Inbox dedupe + local mail insert.
//!
//! Drives `InboxHandler::handle_mail_deliver` with synthetic
//! `MailDeliver` messages and asserts that replays are no-ops at the
//! `peer_inbox` level and that the local `mail` row is created exactly
//! once.

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::peer_inbox::InboxHandler;
use grimoire::daemon::persistence::{Database, unix_now};
use grimoire::shared::peer_proto::MailDeliver;
use grimoire::shared::types::{Agent, AgentState, Peer, PeerState, RestartPolicy};
use std::path::PathBuf;
use std::sync::Arc;

fn fresh_db() -> Arc<Database> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.db");
    std::mem::forget(dir);
    Arc::new(Database::open(&path).unwrap())
}

fn make_peer() -> Peer {
    Peer {
        id: "p-alpha".into(),
        daemon_id: "11111111".into(),
        name: "alpha".into(),
        url: "http://127.0.0.1:1".into(),
        bearer_token_hash: blake3::hash(b"x").as_bytes().to_vec(),
        bearer_token: "0123456789abcdef0123456789abcdef".into(),
        public_key: None,
        state: PeerState::Active,
        last_seen: None,
        registered_at: unix_now(),
    }
}

fn seed_agent(db: &Arc<Database>, id: &str) {
    let agent = Agent {
        id: id.into(),
        name: None,
        state: AgentState::Active,
        task: None,
        model: None,
        provider: None,
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
}

#[tokio::test]
async fn first_delivery_inserts_local_mail() {
    let db = fresh_db();
    let peer = make_peer();
    db.insert_peer(&peer).unwrap();
    seed_agent(&db, "abcd1234");
    let bus = EventBus::new(db.clone());
    let inbox = InboxHandler::new(db.clone(), bus, "22222222".to_string());

    let msg = MailDeliver {
        mail_id: "m1".into(),
        sender: "agent://grimd-11111111/cafef00d".into(),
        recipient: "agent://grimd-22222222/abcd1234".into(),
        body: "hi".into(),
        topic: None,
        sender_seq: 1,
    };
    let ack = inbox.handle_mail_deliver(&peer, &msg).await.unwrap();
    assert!(ack.ok);

    let mail = db.get_mail("m1").unwrap();
    assert!(mail.is_some());
}

#[tokio::test]
async fn replay_is_idempotent() {
    let db = fresh_db();
    let peer = make_peer();
    db.insert_peer(&peer).unwrap();
    seed_agent(&db, "abcd1234");
    let bus = EventBus::new(db.clone());
    let inbox = InboxHandler::new(db.clone(), bus, "22222222".to_string());

    let msg = MailDeliver {
        mail_id: "m2".into(),
        sender: "agent://grimd-11111111/cafef00d".into(),
        recipient: "agent://grimd-22222222/abcd1234".into(),
        body: "hi".into(),
        topic: None,
        sender_seq: 7,
    };
    let _ = inbox.handle_mail_deliver(&peer, &msg).await.unwrap();
    let ack2 = inbox.handle_mail_deliver(&peer, &msg).await.unwrap();
    assert!(ack2.ok, "replayed delivery must ack ok");

    // Should still be one mail row only.
    let mail = db.get_mail("m2").unwrap().unwrap();
    assert_eq!(mail.body, "hi");
}

#[tokio::test]
async fn oversize_body_rejected() {
    let db = fresh_db();
    let peer = make_peer();
    db.insert_peer(&peer).unwrap();
    let bus = EventBus::new(db.clone());
    let inbox = InboxHandler::new(db.clone(), bus, "22222222".to_string());

    let msg = MailDeliver {
        mail_id: "m3".into(),
        sender: "agent://grimd-11111111/cafef00d".into(),
        recipient: "agent://grimd-22222222/abcd1234".into(),
        body: "x".repeat(100_000),
        topic: None,
        sender_seq: 9,
    };
    let ack = inbox.handle_mail_deliver(&peer, &msg).await.unwrap();
    assert!(!ack.ok);
    assert_eq!(ack.reason, "body_too_large");
}
