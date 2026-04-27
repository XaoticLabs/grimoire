//! Cron wake source: fires when the configured 5-field cron expression's
//! next scheduled tick passes the registry's clock.

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CronConfig {
    pub expr: String,
}

pub struct CronSource {
    pub expr: String,
    schedule: ::cron::Schedule,
}

impl CronSource {
    pub fn new(expr: &str) -> Result<Self> {
        // The `cron` crate accepts a 6/7-field schedule (with seconds and
        // year). To accept the 5-field standard cron format the spec calls
        // for, prepend a "0" seconds field if the expression is 5 fields.
        let normalized = normalize_expr(expr);
        let schedule = ::cron::Schedule::from_str(&normalized)
            .map_err(|e| anyhow!("invalid_cron: {}", e))?;
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
        let mut iter = self.schedule.after(&since);
        match iter.next() {
            Some(next) if next <= now => Some(now),
            _ => None,
        }
    }
}

fn normalize_expr(expr: &str) -> String {
    let count = expr.split_whitespace().count();
    if count == 5 {
        format!("0 {}", expr)
    } else {
        expr.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn invalid_cron_returns_err() {
        assert!(CronSource::new("not a cron").is_err());
    }

    #[test]
    fn five_field_cron_accepted() {
        assert!(CronSource::new("0 9 * * 1-5").is_ok());
    }

    #[test]
    fn evaluate_fires_when_clock_crosses_minute_boundary() {
        let s = CronSource::new("* * * * *").unwrap();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 1, 0).unwrap();
        // last_fired_at = t0 (just registered or just fired). At t0 the next
        // tick is in the future; no fire. At t1 we've crossed the next
        // boundary; fire.
        assert!(s.evaluate(t0, Some(t0), t0).is_none());
        assert!(s.evaluate(t1, Some(t0), t0).is_some());
    }

    #[test]
    fn evaluate_at_most_one_fire() {
        let s = CronSource::new("* * * * *").unwrap();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let later = Utc.with_ymd_and_hms(2026, 1, 1, 13, 0, 0).unwrap();
        // Even though many minutes have passed since t0, evaluate returns
        // a single Some — the registry caller is responsible for advancing
        // last_fired_at.
        assert!(s.evaluate(later, Some(t0), t0).is_some());
    }
}
