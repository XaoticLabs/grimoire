//! Contract tests for outbox state transitions and restart reconciliation.
//! These exercise the persistence layer directly, full-stream tests live
//! in `peer_e2e.rs`.

use grimoire::daemon::peer_outbox::backoff_secs;
use grimoire::daemon::persistence::{Database, unix_now};
use grimoire::shared::types::{Mail, MailState, Peer, PeerOutboxState, PeerState};
use std::sync::Arc;

fn fresh_db() -> Arc<Database> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.db");
    std::mem::forget(dir);
    Arc::new(Database::open(&path).unwrap())
}

fn peer_with(name: &str) -> Peer {
    Peer {
        id: format!("p-{name}"),
        daemon_id: "12345678".into(),
        name: name.into(),
        url: "http://127.0.0.1:1".into(),
        bearer_token_hash: blake3::hash(b"x").as_bytes().to_vec(),
        bearer_token: "0123456789abcdef0123456789abcdef".into(),
        public_key: None,
        state: PeerState::Active,
        last_seen: None,
        registered_at: unix_now(),
    }
}

fn seed_outbox_row(db: &Arc<Database>, peer_id: &str, mail_id: &str, outbox_id: &str) {
    let mail = Mail {
        id: mail_id.into(),
        recipient_id: "agent://grimd-12345678/abcd1234".into(),
        sender_id: None,
        topic: None,
        body: "hi".into(),
        in_reply_to: None,
        state: MailState::Pending,
        fail_reason: None,
        created_at: unix_now(),
        delivered_at: None,
        seq: 0,
        wake_eligible: true,
    };
    db.insert_mail_with_outbox(
        &mail,
        peer_id,
        outbox_id,
        "agent://grimd-12345678/abcd1234",
        None,
        unix_now(),
    )
    .unwrap();
}

#[test]
fn pending_to_delivered_on_ack_ok() {
    let db = fresh_db();
    let peer = peer_with("alpha");
    db.insert_peer(&peer).unwrap();
    seed_outbox_row(&db, &peer.id, "m1", "ob1");

    let row = db
        .next_outbox_row(&peer.id, unix_now() + 1)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, PeerOutboxState::Pending);
    db.mark_outbox_in_flight(&row.id).unwrap();
    db.mark_outbox_delivered(&row.id).unwrap();
    // depth counts pending+in_flight only; delivered rows drop out
    assert_eq!(db.outbox_depth(&peer.id).unwrap(), 0);
}

#[test]
fn failure_retries_with_backoff() {
    let db = fresh_db();
    let peer = peer_with("beta");
    db.insert_peer(&peer).unwrap();
    seed_outbox_row(&db, &peer.id, "m2", "ob2");

    let row = db
        .next_outbox_row(&peer.id, unix_now() + 1)
        .unwrap()
        .unwrap();
    db.mark_outbox_in_flight(&row.id).unwrap();
    db.mark_outbox_failed_retry(&row.id, unix_now() + 100)
        .unwrap();

    // next_attempt_at is in the future, so the row is not yet eligible
    let later = db.next_outbox_row(&peer.id, unix_now()).unwrap();
    assert!(later.is_none(), "row should be hidden until retry deadline");

    let due = db
        .next_outbox_row(&peer.id, unix_now() + 200)
        .unwrap()
        .expect("row should reappear after deadline");
    assert_eq!(due.state, PeerOutboxState::Pending);
    assert_eq!(due.attempts, 1);
}

#[test]
fn in_flight_resets_to_pending_on_boot() {
    let db = fresh_db();
    let peer = peer_with("gamma");
    db.insert_peer(&peer).unwrap();
    seed_outbox_row(&db, &peer.id, "m3", "ob3");
    let row = db
        .next_outbox_row(&peer.id, unix_now() + 1)
        .unwrap()
        .unwrap();
    db.mark_outbox_in_flight(&row.id).unwrap();
    let n = db.reset_outbox_in_flight().unwrap();
    assert_eq!(n, 1);
    let again = db
        .next_outbox_row(&peer.id, unix_now() + 1)
        .unwrap()
        .unwrap();
    assert_eq!(again.state, PeerOutboxState::Pending);
}

#[test]
fn backoff_caps_at_60() {
    assert_eq!(backoff_secs(1), 1);
    assert_eq!(backoff_secs(7), 60);
    assert_eq!(backoff_secs(20), 60);
}
