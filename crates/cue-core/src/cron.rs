use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Cron schedule expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronSchedule {
    /// `every 5m` — repeating interval.
    Interval(Duration),
    /// `at 09:00 [on weekdays]` — specific time with optional day filter.
    TimeOfDay {
        /// Seconds from midnight.
        time_secs: u32,
        days: Option<DayFilter>,
    },
    /// `in 30s` — one-shot delay, auto-removed after trigger.
    Delay(Duration),
    /// `daily`, `hourly`, `weekly`, `monthly`.
    Preset(CronPreset),
    /// `cron "*/5 * * * *"` — standard crontab expression.
    Crontab(String),
    /// `<free> do <cmd>` — fallback for unparsable schedule text.
    FreeForm(String),
}

/// Named schedule presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronPreset {
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

/// Day-of-week filter for `at` schedules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DayFilter {
    pub days: Vec<Weekday>,
}

/// Days of the week.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Weekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl CronSchedule {
    /// Whether this is a one-shot schedule (should be removed after trigger).
    pub fn is_oneshot(&self) -> bool {
        matches!(self, Self::Delay(_))
    }
}
