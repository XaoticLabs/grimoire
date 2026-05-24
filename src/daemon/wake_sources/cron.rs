//! Cron wake source: fires when the configured 5-field cron expression's
//! next scheduled tick passes the registry's clock.
//!
//! ## Format
//!
//! Standard 5-field cron: `minute hour day-of-month month day-of-week` (UTC).
//! Each field supports `*`, `N`, `N-M`, `*/K`, `N-M/K`, and comma-separated
//! lists of any of those. Day-of-week is `0-6` with `0 = Sunday`.
//!
//! ## Why not the `cron` crate
//!
//! The `cron` crate pulls in `phf`, `phf_macros`, `phf_generator`, `rand` and
//! `winnow` — none of which would otherwise be in this tree. Our needs are a
//! parser and "next fire time after T"; this hand-rolled implementation is
//! ~150 lines with no transitive deps.

use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CronConfig {
    pub expr: String,
}

pub struct CronSource {
    pub expr: String,
    schedule: Schedule,
}

impl CronSource {
    pub fn new(expr: &str) -> Result<Self> {
        let schedule = Schedule::parse(expr)?;
        Ok(Self {
            expr: expr.to_string(),
            schedule,
        })
    }

    /// Returns `Some(now)` if the source should fire — i.e. at least one
    /// scheduled time exists in the half-open interval `(since, now]`
    /// where `since` is `last_fired_at` if known else the source's
    /// `registered_at`.
    /// At most one fire per evaluation; missed fires beyond one are not
    /// replayed (the catch-up rule).
    pub fn evaluate(
        &self,
        now: DateTime<Utc>,
        last_fired_at: Option<DateTime<Utc>>,
        registered_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let since = last_fired_at.unwrap_or(registered_at);
        match self.schedule.next_after(since) {
            Some(next) if next <= now => Some(now),
            _ => None,
        }
    }
}

/// Parsed 5-field cron expression.
#[derive(Debug, Clone)]
struct Schedule {
    minute: FieldSet, // 0..=59
    hour: FieldSet,   // 0..=23
    dom: FieldSet,    // 1..=31
    month: FieldSet,  // 1..=12
    dow: FieldSet,    // 0..=6 (Sun=0)
    dom_restricted: bool,
    dow_restricted: bool,
}

#[derive(Debug, Clone)]
struct FieldSet {
    // Bitset over the allowed range; bit i = value i is permitted.
    bits: u64,
}

impl FieldSet {
    const fn contains(&self, v: u32) -> bool {
        self.bits & (1u64 << v) != 0
    }
}

impl Schedule {
    fn parse(expr: &str) -> Result<Self> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(anyhow!(
                "invalid_cron: expected 5 fields, got {}",
                parts.len()
            ));
        }
        let minute = parse_field(parts[0], 0, 59)?;
        let hour = parse_field(parts[1], 0, 23)?;
        let dom = parse_field(parts[2], 1, 31)?;
        let month = parse_field(parts[3], 1, 12)?;
        let dow = parse_field(parts[4], 0, 6)?;
        Ok(Self {
            minute,
            hour,
            dom,
            month,
            dow,
            dom_restricted: parts[2] != "*",
            dow_restricted: parts[4] != "*",
        })
    }

    /// Smallest `DateTime<Utc>` strictly greater than `since` matching the
    /// schedule. Returns `None` if no match within ~5 years (a safety cap; in
    /// practice every valid cron fires far sooner).
    fn next_after(&self, since: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Walk minute by minute from `since + 1min`, truncating sub-minute.
        let start = since
            .with_second(0)?
            .with_nanosecond(0)?
            .checked_add_signed(Duration::minutes(1))?;
        let cap = since.checked_add_signed(Duration::days(5 * 366))?;
        let mut t = start;
        while t <= cap {
            if self.matches(t) {
                return Some(t);
            }
            // Skip ahead: if the date is out of range, jump to the next day at
            // 00:00 to avoid 1440 wasted minute-checks.
            if !self.date_matches(t) {
                let next_day = (t.date_naive() + chrono::Duration::days(1))
                    .and_hms_opt(0, 0, 0)?
                    .and_utc();
                t = next_day;
                continue;
            }
            t += Duration::minutes(1);
        }
        None
    }

    fn matches(&self, t: DateTime<Utc>) -> bool {
        self.minute.contains(t.minute()) && self.hour.contains(t.hour()) && self.date_matches(t)
    }

    fn date_matches(&self, t: DateTime<Utc>) -> bool {
        if !self.month.contains(t.month()) {
            return false;
        }
        let day_match = self.dom.contains(t.day());
        // chrono weekday: Monday=0..Sunday=6. Cron weekday: Sunday=0..Saturday=6.
        let weekday_match = self.dow.contains(t.weekday().num_days_from_sunday());
        // Per POSIX cron, when BOTH dom and dow are restricted, the match is
        // an OR. When only one is restricted, it's the active filter. When
        // neither is restricted, both are "*" and trivially match.
        match (self.dom_restricted, self.dow_restricted) {
            (true, true) => day_match || weekday_match,
            _ => day_match && weekday_match,
        }
    }
}

fn parse_field(spec: &str, min: u32, max: u32) -> Result<FieldSet> {
    let mut bits: u64 = 0;
    for part in spec.split(',') {
        let (range_spec, step) = match part.split_once('/') {
            Some((r, s)) => (
                r,
                s.parse::<u32>()
                    .map_err(|_| anyhow!("invalid_cron: bad step '{s}'"))?,
            ),
            None => (part, 1),
        };
        if step == 0 {
            return Err(anyhow!("invalid_cron: step must be > 0"));
        }
        let (lo, hi) = if range_spec == "*" {
            (min, max)
        } else if let Some((a, b)) = range_spec.split_once('-') {
            let a: u32 = a
                .parse()
                .map_err(|_| anyhow!("invalid_cron: bad range start '{a}'"))?;
            let b: u32 = b
                .parse()
                .map_err(|_| anyhow!("invalid_cron: bad range end '{b}'"))?;
            (a, b)
        } else {
            let v: u32 = range_spec
                .parse()
                .map_err(|_| anyhow!("invalid_cron: bad value '{range_spec}'"))?;
            // Bare N with /K means N, N+K, N+2K, ... up to max.
            if step > 1 { (v, max) } else { (v, v) }
        };
        if lo < min || hi > max || lo > hi {
            return Err(anyhow!("invalid_cron: '{spec}' out of range {min}..={max}"));
        }
        let mut v = lo;
        while v <= hi {
            bits |= 1u64 << v;
            v += step;
        }
    }
    if bits == 0 {
        return Err(anyhow!("invalid_cron: empty field '{spec}'"));
    }
    Ok(FieldSet { bits })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn invalid_cron_returns_err() {
        assert!(CronSource::new("not a cron").is_err());
        assert!(CronSource::new("* * * *").is_err()); // 4 fields
        assert!(CronSource::new("60 * * * *").is_err()); // minute out of range
        assert!(CronSource::new("* */0 * * *").is_err()); // zero step
    }

    #[test]
    fn five_field_cron_accepted() {
        assert!(CronSource::new("0 9 * * 1-5").is_ok());
        assert!(CronSource::new("*/5 * * * *").is_ok());
        assert!(CronSource::new("0,15,30,45 * * * *").is_ok());
    }

    #[test]
    fn evaluate_fires_when_clock_crosses_minute_boundary() {
        let s = CronSource::new("* * * * *").unwrap();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 1, 0).unwrap();
        assert!(s.evaluate(t0, Some(t0), t0).is_none());
        assert!(s.evaluate(t1, Some(t0), t0).is_some());
    }

    #[test]
    fn evaluate_at_most_one_fire() {
        let s = CronSource::new("* * * * *").unwrap();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let later = Utc.with_ymd_and_hms(2026, 1, 1, 13, 0, 0).unwrap();
        assert!(s.evaluate(later, Some(t0), t0).is_some());
    }

    #[test]
    fn weekday_filter_matches_business_days() {
        // 0 9 * * 1-5 — 09:00 UTC, Mon–Fri.
        let s = CronSource::new("0 9 * * 1-5").unwrap();
        // 2026-01-05 is a Monday.
        let mon = Utc.with_ymd_and_hms(2026, 1, 5, 8, 59, 0).unwrap();
        let next = s.schedule.next_after(mon).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 5, 9, 0, 0).unwrap());
        // After Friday 09:00, next is Monday 09:00 (skip Sat/Sun).
        let fri = Utc.with_ymd_and_hms(2026, 1, 9, 9, 0, 0).unwrap();
        let next_fri = s.schedule.next_after(fri).unwrap();
        assert_eq!(
            next_fri,
            Utc.with_ymd_and_hms(2026, 1, 12, 9, 0, 0).unwrap()
        );
    }

    #[test]
    fn step_field_matches_every_nth() {
        let s = CronSource::new("*/15 * * * *").unwrap();
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let next = s.schedule.next_after(t).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 12, 15, 0).unwrap());
    }

    #[test]
    fn list_field_picks_smallest_after() {
        let s = CronSource::new("0,30 * * * *").unwrap();
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 12, 5, 0).unwrap();
        let next = s.schedule.next_after(t).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 12, 30, 0).unwrap());
    }
}
