use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use crate::daemon::persistence::Database;
use crate::shared::protocol::StreamEvent;

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<StreamEvent>,
    writer: mpsc::UnboundedSender<StreamEvent>,
}

impl EventBus {
    pub fn new(db: Arc<Database>) -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (writer, mut rx) = mpsc::unbounded_channel::<StreamEvent>();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Err(err) = db.append_event(&event) {
                    tracing::error!(?err, "failed to persist event");
                }
            }
        });
        Self { sender, writer }
    }

    pub fn publish(&self, event: StreamEvent) {
        // Ignore "no receivers" and "writer gone" (shutdown).
        let _ = self.sender.send(event.clone());
        let _ = self.writer.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.sender.subscribe()
    }
}
