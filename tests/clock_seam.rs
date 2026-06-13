//! Contract tests for the `Clock` trait + `SystemClock` + `TestClock`.

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use grimoire::daemon::clock::{Clock, SystemClock, TestClock};

#[test]
fn system_clock_now_within_one_second_of_utc_now() {
    let c = SystemClock;
    let before = Utc::now();
    let t = c.now();
    let after = Utc::now();
    assert!(t >= before - Duration::milliseconds(50));
    assert!(t <= after + Duration::milliseconds(50));
}

#[test]
fn test_clock_advance_shifts_now() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let c = TestClock::new(t0);
    assert_eq!(c.now(), t0);
    c.advance(Duration::hours(1));
    assert_eq!(c.now(), t0 + Duration::hours(1));
}

#[test]
fn test_clock_set_overwrites_now() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2027, 6, 15, 12, 0, 0).unwrap();
    let c = TestClock::new(t0);
    c.set(t1);
    assert_eq!(c.now(), t1);
}

#[test]
fn test_clock_is_send_sync() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let c: Arc<dyn Clock> = Arc::new(TestClock::new(t0));
    assert_eq!(c.now(), t0);
}
