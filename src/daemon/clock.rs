//! Clock seam used by the wake registry, cron evaluator, and rate limiter
//! so tests can drive time deterministically.

use chrono::{DateTime, Utc};
use parking_lot::Mutex;

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct TestClock {
    inner: Mutex<DateTime<Utc>>,
}

impl TestClock {
    pub const fn new(t: DateTime<Utc>) -> Self {
        Self {
            inner: Mutex::new(t),
        }
    }

    pub fn advance(&self, d: chrono::Duration) {
        let mut g = self.inner.lock();
        *g += d;
    }

    pub fn set(&self, t: DateTime<Utc>) {
        let mut g = self.inner.lock();
        *g = t;
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.inner.lock()
    }
}
