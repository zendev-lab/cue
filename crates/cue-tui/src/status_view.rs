use cue_core::cron::CronStatus;
use cue_core::execution::{ExecutionCancelReason, ExecutionState};

use crate::component::main_view::CardStatus;

pub(crate) fn job_status_text(status: &ExecutionState) -> String {
    match status {
        ExecutionState::Queued => "pending".to_string(),
        ExecutionState::Running => "running".to_string(),
        ExecutionState::Succeeded => "succeeded".to_string(),
        ExecutionState::Failed => "failed".to_string(),
        ExecutionState::Cancelled { reason } => format!("cancelled({reason:?})").to_lowercase(),
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

pub(crate) fn job_card_status(status: &ExecutionState) -> CardStatus {
    match status {
        ExecutionState::Queued => CardStatus::Pending,
        ExecutionState::Running => CardStatus::Streaming,
        ExecutionState::Succeeded => CardStatus::Success,
        ExecutionState::Failed | ExecutionState::Cancelled { .. } => CardStatus::Error,
    }
}

pub(crate) fn job_status_icon(status: &ExecutionState) -> &'static str {
    match status {
        ExecutionState::Queued => "⏳",
        ExecutionState::Running => "🔄",
        ExecutionState::Succeeded => "✅",
        ExecutionState::Failed => "❌",
        ExecutionState::Cancelled {
            reason: ExecutionCancelReason::Forced,
        } => "🛑",
        ExecutionState::Cancelled { .. } => "⏹",
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
        assert_eq!(job_status_text(&ExecutionState::Running), "running");
        assert_eq!(
            job_status_icon(&ExecutionState::Cancelled {
                reason: ExecutionCancelReason::Forced,
            }),
            "🛑"
        );
        assert_eq!(
            job_card_status(&ExecutionState::Succeeded),
            CardStatus::Success
        );
        assert_eq!(
            job_card_status(&ExecutionState::Cancelled {
                reason: ExecutionCancelReason::User,
            }),
            CardStatus::Error
        );
    }

    #[test]
    fn cron_status_maps_to_text_and_icon() {
        assert_eq!(cron_status_text(CronStatus::Paused), "paused");
        assert_eq!(cron_status_icon(CronStatus::Expired), "⌛");
    }
}
