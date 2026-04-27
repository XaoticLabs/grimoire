//! Federation inbound mail handler (Task 9).
//!
//! When a `MailDeliver` arrives over the peer channel:
//! 1. Validate body size and recipient shape.
//! 2. Idempotency-keyed insert into `peer_inbox` on `(sender_daemon_id, sender_seq)`.
//! 3. If new (not a replay): insert the local `mail` row.
//! 4. Emit `PeerMailReceived` and `MailReceived` events. The scheduler's
//!    existing `tick_mail_wake` path picks up wake-on-mail next tick.

use anyhow::Result;
use std::sync::Arc;

use crate::shared::mail::{Address, parse_address};
use crate::shared::peer_proto::{MailAck, MailDeliver};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentState, DaemonId, Mail, MailState, Peer};

use super::event_bus::EventBus;
use super::persistence::{Database, unix_now};

const MAX_MAIL_BODY_BYTES: usize = 65_536;
const PREVIEW_CHARS: usize = 200;

pub struct InboxHandler {
    db: Arc<Database>,
    bus: EventBus,
    daemon_id: DaemonId,
}

impl InboxHandler {
    pub fn new(db: Arc<Database>, bus: EventBus, daemon_id: DaemonId) -> Self {
        Self { db, bus, daemon_id }
    }

    pub async fn handle_mail_deliver(
        &self,
        peer: &Peer,
        msg: &MailDeliver,
    ) -> Result<MailAck> {
        if msg.body.len() > MAX_MAIL_BODY_BYTES {
            return Ok(ack_fail(&msg.mail_id, "body_too_large"));
        }
        let address = match parse_address(&msg.recipient) {
            Ok(a) => a,
            Err(_) => return Ok(ack_fail(&msg.mail_id, "invalid_recipient")),
        };

        // Idempotency check up front — if we've already seen this
        // (sender_daemon_id, sender_seq), ack ok and don't re-insert.
        let inserted = self.db.insert_peer_inbox_if_absent(
            &peer.daemon_id,
            msg.sender_seq,
            &msg.mail_id,
            unix_now(),
        )?;
        if !inserted {
            return Ok(ack_ok(&msg.mail_id));
        }

        match address {
            Address::FederatedAgent { daemon_id, agent_id } => {
                if daemon_id != self.daemon_id {
                    return Ok(ack_fail(&msg.mail_id, "invalid_recipient"));
                }
                self.deliver_direct(peer, msg, &agent_id).await
            }
            Address::Agent(agent_id) => {
                // Bare local form is also accepted (server's daemon-id
                // collapsed at the sender).
                self.deliver_direct(peer, msg, &agent_id).await
            }
            Address::Topic(topic) => {
                // Inbound topic federation: requires explicit Inbound/Both
                // authorization on this side (federation Task 12).
                if !self
                    .db
                    .topic_federation_inbound_authorized(&peer.id, &topic)?
                {
                    return Ok(ack_fail(&msg.mail_id, "topic_federation_not_authorized"));
                }
                self.deliver_topic(peer, msg, &topic).await
            }
        }
    }

    async fn deliver_direct(
        &self,
        peer: &Peer,
        msg: &MailDeliver,
        agent_id: &str,
    ) -> Result<MailAck> {
        let agent = self.db.get_agent(agent_id)?;
        let now = unix_now();
        let (state, fail_reason): (MailState, Option<&'static str>) = match agent {
            None => (MailState::Failed, Some("unknown_recipient")),
            Some(a) if a.state == AgentState::Banished => {
                (MailState::Failed, Some("recipient_banished"))
            }
            Some(_) => (MailState::Pending, None),
        };
        let mail = Mail {
            id: msg.mail_id.clone(),
            recipient_id: agent_id.to_string(),
            sender_id: Some(msg.sender.clone()),
            topic: None,
            body: msg.body.clone(),
            in_reply_to: None,
            state,
            fail_reason: fail_reason.map(|s| s.to_string()),
            created_at: now,
            delivered_at: if state != MailState::Pending {
                Some(now)
            } else {
                None
            },
            seq: 0,
            wake_eligible: true,
        };
        self.db.insert_mail(&mail)?;

        if let Some(reason) = fail_reason {
            return Ok(ack_fail(&msg.mail_id, reason));
        }

        let preview: String = msg.body.chars().take(PREVIEW_CHARS).collect();
        self.bus.publish(StreamEvent::PeerMailReceived {
            peer_id: peer.id.clone(),
            mail_id: msg.mail_id.clone(),
            sender_daemon_id: peer.daemon_id.clone(),
        });
        self.bus.publish(StreamEvent::MailReceived {
            mail_id: msg.mail_id.clone(),
            recipient_id: agent_id.to_string(),
            sender_id: Some(msg.sender.clone()),
            topic: None,
            body_preview: preview,
            wake_eligible: true,
            origin_daemon_id: Some(peer.daemon_id.clone()),
        });
        Ok(ack_ok(&msg.mail_id))
    }

    async fn deliver_topic(
        &self,
        peer: &Peer,
        msg: &MailDeliver,
        topic: &str,
    ) -> Result<MailAck> {
        // Local topic fanout — one mail row per local subscriber.
        let subscribers = self.db.list_subscribers_for_topic(topic)?;
        if subscribers.is_empty() {
            return Ok(ack_ok(&msg.mail_id));
        }
        let now = unix_now();
        let preview: String = msg.body.chars().take(PREVIEW_CHARS).collect();
        let mut mails = Vec::with_capacity(subscribers.len());
        for sub in &subscribers {
            let agent = self.db.get_agent(&sub.subscriber_id)?;
            let (state, fail_reason): (MailState, Option<&'static str>) = match agent {
                None => (MailState::Failed, Some("unknown_recipient")),
                Some(a) if a.state == AgentState::Banished => {
                    (MailState::Failed, Some("recipient_banished"))
                }
                Some(_) => (MailState::Pending, None),
            };
            let mail = Mail {
                id: crate::shared::constants::generate_short_id(),
                recipient_id: sub.subscriber_id.clone(),
                sender_id: Some(msg.sender.clone()),
                topic: Some(topic.to_string()),
                body: msg.body.clone(),
                in_reply_to: None,
                state,
                fail_reason: fail_reason.map(|s| s.to_string()),
                created_at: now,
                delivered_at: if state != MailState::Pending {
                    Some(now)
                } else {
                    None
                },
                seq: 0,
                wake_eligible: true,
            };
            mails.push(mail);
        }
        self.db.insert_mail_batch(&mails)?;
        for m in &mails {
            if m.state == MailState::Pending {
                self.bus.publish(StreamEvent::MailReceived {
                    mail_id: m.id.clone(),
                    recipient_id: m.recipient_id.clone(),
                    sender_id: Some(msg.sender.clone()),
                    topic: Some(topic.to_string()),
                    body_preview: preview.clone(),
                    wake_eligible: true,
                    origin_daemon_id: Some(peer.daemon_id.clone()),
                });
            }
        }
        self.bus.publish(StreamEvent::PeerMailReceived {
            peer_id: peer.id.clone(),
            mail_id: msg.mail_id.clone(),
            sender_daemon_id: peer.daemon_id.clone(),
        });
        Ok(ack_ok(&msg.mail_id))
    }
}

fn ack_ok(mail_id: &str) -> MailAck {
    MailAck {
        mail_id: mail_id.to_string(),
        ok: true,
        reason: String::new(),
    }
}

fn ack_fail(mail_id: &str, reason: &str) -> MailAck {
    MailAck {
        mail_id: mail_id.to_string(),
        ok: false,
        reason: reason.to_string(),
    }
}
