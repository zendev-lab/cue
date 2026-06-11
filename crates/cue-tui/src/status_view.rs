use cue_core::cron::CronStatus;
use cue_core::ipc::ChainInfo;
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

pub(crate) fn chain_card_status(chain: &ChainInfo) -> CardStatus {
    if chain.jobs.iter().any(|job| {
        matches!(
            job.status,
            JobStatus::Failed | JobStatus::Killed | JobStatus::Cancelled(_)
        )
    }) {
        return CardStatus::Error;
    }
    if chain
        .jobs
        .iter()
        .any(|job| job.status == JobStatus::Running)
    {
        return CardStatus::Streaming;
    }
    if chain.total_jobs > 0
        && chain.jobs.len() == chain.total_jobs
        && chain.jobs.iter().all(|job| job.status == JobStatus::Done)
    {
        return CardStatus::Success;
    }
    CardStatus::Pending
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
    use cue_core::ipc::ChainJobInfo;

    fn chain(statuses: Vec<JobStatus>) -> ChainInfo {
        ChainInfo {
            id: "CH1".into(),
            pipeline: "build -> test".into(),
            total_jobs: statuses.len(),
            jobs: statuses
                .into_iter()
                .enumerate()
                .map(|(index, status)| ChainJobInfo {
                    index,
                    pipeline: format!("step {index}"),
                    status,
                    job_id: None,
                    start_scope: None,
                    end_scope: None,
                    open_hint: None,
                })
                .collect(),
        }
    }

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
    fn chain_card_status_prioritizes_errors_then_running_then_success() {
        assert_eq!(
            chain_card_status(&chain(vec![JobStatus::Done, JobStatus::Failed])),
            CardStatus::Error
        );
        assert_eq!(
            chain_card_status(&chain(vec![JobStatus::Done, JobStatus::Running])),
            CardStatus::Streaming
        );
        assert_eq!(
            chain_card_status(&chain(vec![JobStatus::Done, JobStatus::Done])),
            CardStatus::Success
        );
    }

    #[test]
    fn cron_status_maps_to_text_and_icon() {
        assert_eq!(cron_status_text(CronStatus::Paused), "paused");
        assert_eq!(cron_status_icon(CronStatus::Expired), "⌛");
    }
}
