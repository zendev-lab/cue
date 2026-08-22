use cue_core::cron::CronStatus;
use cue_core::job::JobStatus;

use crate::component::main_view::CardStatus;

pub(crate) fn job_status_text(status: &JobStatus) -> String {
    match status {
        JobStatus::Pending => "pending".to_string(),
        JobStatus::Running => "running".to_string(),
        JobStatus::Done => "done".to_string(),
        JobStatus::Failed => "failed".to_string(),
        JobStatus::Killed => "killed".to_string(),
        JobStatus::Cancelled(reason) => format!("cancelled({reason:?})").to_lowercase(),
    }
}

pub(crate) fn cron_status_text(status: CronStatus) -> &'static str {
    match status {
        CronStatus::Scheduled => "scheduled",
        CronStatus::Paused => "paused",
        CronStatus::Completed => "completed",
        CronStatus::Expired => "expired",
        CronStatus::Failed => "failed",
    }
}

pub(crate) fn job_card_status(status: &JobStatus) -> CardStatus {
    match status {
        JobStatus::Pending => CardStatus::Pending,
        JobStatus::Running => CardStatus::Streaming,
        JobStatus::Done => CardStatus::Success,
        JobStatus::Failed | JobStatus::Killed | JobStatus::Cancelled(_) => CardStatus::Error,
    }
}

pub(crate) fn job_status_icon(status: &JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "⏳",
        JobStatus::Running => "🔄",
        JobStatus::Done => "✅",
        JobStatus::Failed => "❌",
        JobStatus::Killed => "🛑",
        JobStatus::Cancelled(_) => "⏹",
    }
}

pub(crate) fn cron_status_icon(status: CronStatus) -> &'static str {
    match status {
        CronStatus::Scheduled => "⏰",
        CronStatus::Paused => "⏸",
        CronStatus::Completed => "✅",
        CronStatus::Expired => "⌛",
        CronStatus::Failed => "✖",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn job_status_maps_to_text_icon_and_card_status() {
        assert_eq!(job_status_text(&JobStatus::Running), "running");
        assert_eq!(job_status_icon(&JobStatus::Killed), "🛑");
        assert_eq!(job_card_status(&JobStatus::Done), CardStatus::Success);
        assert_eq!(
            job_card_status(&JobStatus::Cancelled(cue_core::job::CancelReason::User)),
            CardStatus::Error
        );
    }

    #[test]
    fn cron_status_maps_to_text_and_icon() {
        assert_eq!(cron_status_text(CronStatus::Paused), "paused");
        assert_eq!(cron_status_icon(CronStatus::Expired), "⌛");
    }
}
