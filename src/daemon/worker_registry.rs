use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use semver::{Version, VersionReq};
use tokio::sync::mpsc;

use crate::daemon::event_bus::EventBus;
use crate::shared::protocol::StreamEvent;
use crate::shared::worker_proto::DaemonMessage;

pub type WorkerId = String;

pub const MIN_WORKER_VERSION: &str = "0.1.0";

// `worker_id` matches `RegisterParams`, the proto wire format, and the schema.
#[allow(clippy::struct_field_names)]
pub struct Worker {
    pub worker_id: WorkerId,
    pub providers: Vec<(String, Version)>,
    pub tags: Vec<String>,
    pub max_concurrent: u32,
    pub in_flight: u32,
    pub last_heartbeat: Instant,
    pub assign_tx: mpsc::Sender<DaemonMessage>,
}

pub struct RegisterParams {
    pub worker_id: String,
    pub bearer_ok: bool,
    pub worker_version: String,
    pub max_concurrent: u32,
    pub providers: Vec<(String, Version)>,
    pub tags: Vec<String>,
    pub assign_tx: mpsc::Sender<DaemonMessage>,
}

#[derive(Default)]
struct ClockState {
    enabled: bool,
    offset: Duration,
}

pub struct WorkerRegistry {
    workers: Mutex<HashMap<WorkerId, Worker>>,
    eviction_after: Duration,
    clock: Mutex<ClockState>,
    bus: Option<EventBus>,
}

impl WorkerRegistry {
    pub fn new(eviction_after: Duration) -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
            eviction_after,
            clock: Mutex::new(ClockState::default()),
            bus: None,
        }
    }

    pub fn new_with_bus(eviction_after: Duration, bus: EventBus) -> Self {
        let mut r = Self::new(eviction_after);
        r.bus = Some(bus);
        r
    }

    pub fn new_with_clock_for_test(eviction_after: Duration) -> Self {
        let r = Self::new(eviction_after);
        r.clock.lock().enabled = true;
        r
    }

    pub fn advance_clock_for_test(&self, by: Duration) {
        let mut c = self.clock.lock();
        c.offset += by;
    }

    fn now(&self) -> Instant {
        let c = self.clock.lock();
        if c.enabled {
            // Test clock: advancing the offset makes last_heartbeat look older.
            Instant::now() + c.offset
        } else {
            Instant::now()
        }
    }

    pub fn register(&self, params: RegisterParams) -> Result<()> {
        if !params.bearer_ok {
            return Err(anyhow!("bad bearer token"));
        }
        let mut workers = self.workers.lock();
        if workers.contains_key(&params.worker_id) {
            return Err(anyhow!("worker already registered: {}", params.worker_id));
        }
        let now = if self.clock.lock().enabled {
            Instant::now() + self.clock.lock().offset
        } else {
            Instant::now()
        };
        let worker_id = params.worker_id.clone();
        let worker = Worker {
            worker_id: params.worker_id.clone(),
            providers: params.providers,
            tags: params.tags,
            max_concurrent: params.max_concurrent,
            in_flight: 0,
            last_heartbeat: now,
            assign_tx: params.assign_tx,
        };
        workers.insert(params.worker_id, worker);
        drop(workers);
        if let Some(bus) = &self.bus {
            bus.publish(StreamEvent::WorkerRegistered { worker_id });
        }
        Ok(())
    }

    pub fn evict(&self, worker_id: &str) {
        let mut workers = self.workers.lock();
        workers.remove(worker_id);
    }

    pub fn record_heartbeat(&self, worker_id: &str, in_flight: u32) {
        let mut workers = self.workers.lock();
        if let Some(w) = workers.get_mut(worker_id) {
            w.in_flight = in_flight;
            w.last_heartbeat = if self.clock.lock().enabled {
                Instant::now() + self.clock.lock().offset
            } else {
                Instant::now()
            };
        }
    }

    pub fn set_in_flight_for_test(&self, worker_id: &str, in_flight: u32) {
        let mut workers = self.workers.lock();
        if let Some(w) = workers.get_mut(worker_id) {
            w.in_flight = in_flight;
        }
    }

    pub fn count(&self) -> usize {
        self.workers.lock().len()
    }

    pub fn has_eligible_worker(&self, provider_name: &str, constraint: &VersionReq) -> bool {
        let workers = self.workers.lock();
        workers.values().any(|w| {
            w.providers
                .iter()
                .any(|(n, v)| n == provider_name && constraint.matches(v))
        })
    }

    pub fn pick_least_loaded(
        &self,
        provider_name: &str,
        constraint: &VersionReq,
    ) -> Option<WorkerId> {
        let workers = self.workers.lock();
        let mut candidates: Vec<&Worker> = workers
            .values()
            .filter(|w| w.in_flight < w.max_concurrent)
            .filter(|w| {
                w.providers
                    .iter()
                    .any(|(n, v)| n == provider_name && constraint.matches(v))
            })
            .collect();
        candidates.sort_by(|a, b| {
            a.in_flight
                .cmp(&b.in_flight)
                .then_with(|| a.worker_id.cmp(&b.worker_id))
        });
        candidates.first().map(|w| w.worker_id.clone())
    }

    pub fn assign_tx(&self, worker_id: &str) -> Option<mpsc::Sender<DaemonMessage>> {
        let workers = self.workers.lock();
        workers.get(worker_id).map(|w| w.assign_tx.clone())
    }

    pub async fn run_eviction_pass_for_test(&self) {
        self.run_eviction_pass();
    }

    pub fn run_eviction_pass(&self) {
        let now = self.now();
        let mut workers = self.workers.lock();
        let stale: Vec<String> = workers
            .iter()
            .filter(|(_, w)| now.saturating_duration_since(w.last_heartbeat) > self.eviction_after)
            .map(|(k, _)| k.clone())
            .collect();
        for id in stale {
            workers.remove(&id);
        }
    }
}

pub fn worker_version_meets_minimum(version_str: &str) -> bool {
    let Ok(ver) = Version::parse(version_str) else {
        return false;
    };
    let Ok(min) = Version::parse(MIN_WORKER_VERSION) else {
        return false;
    };
    ver >= min
}
