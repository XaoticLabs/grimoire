//! Outbound peer client (Tasks 6, 7). One task per peer link:
//! connects, performs the `Hello`/`HelloAck` handshake, runs the
//! heartbeat + select loop, and reconnects with exponential backoff
//! on disconnect.

mod appliers;
mod connection;
mod inbound;
mod outbox;

pub use appliers::{
    AgentLifecyclePayload, apply_agent_lifecycle_deliver, apply_memory_deliver,
    apply_scroll_task_dispatch, apply_workspace_event_deliver,
};
pub use connection::{PeerClientHandle, spawn};
pub use outbox::ScrollDispatchPayload;
