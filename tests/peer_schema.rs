//! Federation Task 3 contract tests: schema, dedupe, cascade.
//!
//! Boots a fresh `Database` against a tempdir and asserts the four
//! federation tables exist and behave per spec (UNIQUE on
//! `(peer_id, sender_seq)`, PK dedupe on `peer_inbox`, FK cascade from
//! `peers` to `peer_outbox`).

use grimoire::daemon::persistence::{Database, unix_now};
use grimoire::shared::types::{Peer, PeerState};
use std::sync::Arc;

fn fresh_db() -> Arc<Database> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("grimoire.db");
    // Leak the tempdir so the file outlives this fn.
    std::mem::forget(dir);
    Arc::new(Database::open(&path).unwrap())
}

fn fake_peer(name: &str, token: &str) -> Peer {
    let token_hash = blake3::hash(token.as_bytes()).as_bytes().to_vec();
    Peer {
        id: format!("p-{}", name),
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
    // Insert a pending mail+outbox row.
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
        .upsert_topic_federation("f1", &peer.id, "pr-opened", FederationDirection::Outbound, unix_now())
        .unwrap();
    assert_eq!(dir1, FederationDirection::Outbound);
    let dir2 = db
        .upsert_topic_federation("f2", &peer.id, "pr-opened", FederationDirection::Inbound, unix_now())
        .unwrap();
    assert_eq!(dir2, FederationDirection::Both);
}
