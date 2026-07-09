use cue_core::ipc::JobOpenHint;
use cue_core::job::JobStatus;

/// Convert a sidebar display row (newest-first) to the underlying vec index (oldest-first).
pub(crate) fn display_row_to_index(row: usize, len: usize) -> Option<usize> {
    len.checked_sub(1)?.checked_sub(row)
}

pub(crate) fn job_open_command(id: &str, status: &JobStatus, open_hint: JobOpenHint) -> String {
    if matches!(status, JobStatus::Running) && open_hint == JobOpenHint::Fg {
        format!(":fg {id}")
    } else {
        format!(":tail {id}")
    }
}

pub(crate) fn running_job_kill_command(id: &str, status: &JobStatus) -> Option<String> {
    matches!(status, JobStatus::Running).then(|| format!(":kill {id}"))
}

pub(crate) fn cron_kill_command(id: &str) -> String {
    format!(":kill {id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_rows_map_newest_first_to_oldest_first_indices() {
        assert_eq!(display_row_to_index(0, 3), Some(2));
        assert_eq!(display_row_to_index(1, 3), Some(1));
        assert_eq!(display_row_to_index(2, 3), Some(0));
        assert_eq!(display_row_to_index(3, 3), None);
        assert_eq!(display_row_to_index(0, 0), None);
    }

    #[test]
    fn job_open_command_attaches_only_foreground_capable_running_jobs() {
        assert_eq!(
            job_open_command("J7", &JobStatus::Running, JobOpenHint::Fg),
            ":fg J7"
        );
        assert_eq!(
            job_open_command("J7", &JobStatus::Running, JobOpenHint::Stream),
            ":tail J7"
        );
        assert_eq!(
            job_open_command("J7", &JobStatus::Done, JobOpenHint::Fg),
            ":tail J7"
        );
        assert_eq!(
            job_open_command("J7", &JobStatus::Failed, JobOpenHint::Stream),
            ":tail J7"
        );
    }

    #[test]
    fn kill_commands_are_limited_to_running_jobs_and_crons() {
        assert_eq!(
            running_job_kill_command("J7", &JobStatus::Running),
            Some(":kill J7".into())
        );
        assert_eq!(running_job_kill_command("J7", &JobStatus::Done), None);
        assert_eq!(cron_kill_command("C2"), ":kill C2");
    }
}
