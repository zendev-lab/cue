use cue_core::Mode;
use cue_core::cron::CronStatus;
use cue_core::job::JobStatus;
use ratatui::layout::Rect;

use crate::geometry::{centered_rect, inner_rect};

#[derive(Debug, Clone)]
pub(crate) struct JobPickerState {
    selected: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JobPickerItem {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) status_icon: &'static str,
}

pub(crate) struct JobPickerRecord<'a> {
    pub(crate) id: &'a str,
    pub(crate) label: &'a str,
    pub(crate) status: &'a JobStatus,
}

pub(crate) struct CronPickerRecord<'a> {
    pub(crate) id: &'a str,
    pub(crate) label: &'a str,
    pub(crate) status: CronStatus,
}

pub(crate) fn job_picker_item(record: JobPickerRecord<'_>) -> Option<JobPickerItem> {
    matches!(record.status, JobStatus::Running).then(|| JobPickerItem {
        id: record.id.to_string(),
        label: record.label.to_string(),
        status_icon: crate::status_view::job_status_icon(record.status),
    })
}

pub(crate) fn cron_picker_item(record: CronPickerRecord<'_>) -> JobPickerItem {
    JobPickerItem {
        id: record.id.to_string(),
        label: record.label.to_string(),
        status_icon: crate::status_view::cron_status_icon(record.status),
    }
}

pub(crate) fn job_picker_popup_rect(area: Rect) -> Rect {
    centered_rect(area, 70, 60)
}

pub(crate) fn job_picker_content_rect(popup: Rect) -> Rect {
    inner_rect(popup)
}

impl JobPickerState {
    pub(crate) fn open(item_count: usize) -> Self {
        Self {
            selected: item_count.checked_sub(1),
        }
    }

    pub(crate) fn selected(&self) -> Option<usize> {
        self.selected
    }

    pub(crate) fn select(&mut self, index: usize) {
        self.selected = Some(index);
    }

    pub(crate) fn move_selection(&mut self, delta: isize, item_count: usize) {
        if item_count == 0 {
            self.selected = None;
            return;
        }

        let current = self.selected.unwrap_or(item_count - 1) as isize;
        let next = (current + delta).clamp(0, item_count.saturating_sub(1) as isize);
        self.selected = Some(next as usize);
    }
}

pub(crate) fn title(mode: Mode) -> &'static str {
    match mode {
        Mode::Job => "Running Jobs",
        Mode::Cron => "Crons",
    }
}

pub(crate) fn empty_text(mode: Mode) -> &'static str {
    match mode {
        Mode::Job => "No running jobs.",
        Mode::Cron => "No crons.",
    }
}

pub(crate) fn submit_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Job => "kill",
        Mode::Cron => "remove",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picker_modal_rects_share_popup_and_content_geometry() {
        let popup = job_picker_popup_rect(Rect::new(0, 0, 100, 50));

        assert_eq!(popup, Rect::new(15, 10, 70, 30));
        assert_eq!(job_picker_content_rect(popup), Rect::new(16, 11, 68, 28));
    }

    #[test]
    fn picker_items_include_running_jobs_and_all_crons() {
        assert_eq!(
            job_picker_item(JobPickerRecord {
                id: "J7",
                label: "server",
                status: &JobStatus::Running,
            }),
            Some(JobPickerItem {
                id: "J7".into(),
                label: "server".into(),
                status_icon: "🔄",
            })
        );
        assert_eq!(
            job_picker_item(JobPickerRecord {
                id: "J8",
                label: "done",
                status: &JobStatus::Done,
            }),
            None
        );
        assert_eq!(
            cron_picker_item(CronPickerRecord {
                id: "C2",
                label: "daily",
                status: CronStatus::Scheduled,
            }),
            JobPickerItem {
                id: "C2".into(),
                label: "daily".into(),
                status_icon: "⏰",
            }
        );
    }

    #[test]
    fn open_selects_last_item() {
        assert_eq!(JobPickerState::open(3).selected(), Some(2));
        assert_eq!(JobPickerState::open(0).selected(), None);
    }

    #[test]
    fn movement_clamps_to_available_items() {
        let mut picker = JobPickerState::open(3);

        picker.move_selection(10, 3);
        assert_eq!(picker.selected(), Some(2));

        picker.move_selection(-10, 3);
        assert_eq!(picker.selected(), Some(0));
    }

    #[test]
    fn empty_items_clear_selection() {
        let mut picker = JobPickerState::open(3);

        picker.move_selection(1, 0);

        assert_eq!(picker.selected(), None);
    }

    #[test]
    fn mode_labels_describe_picker_action() {
        assert_eq!(title(Mode::Job), "Running Jobs");
        assert_eq!(empty_text(Mode::Cron), "No crons.");
        assert_eq!(submit_label(Mode::Cron), "remove");
    }
}
