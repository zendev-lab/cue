use cue_core::cron::CronStatus;
use cue_core::execution::ExecutionState;
use cue_language::Mode;

use crate::component::main_view::{Card, CardStatus};
use crate::status_view;

pub(crate) struct JobRecord<'a> {
    pub(crate) id: &'a str,
    pub(crate) status: &'a ExecutionState,
    pub(crate) start_scope: Option<&'a str>,
    pub(crate) end_scope: Option<&'a str>,
    pub(crate) warnings: &'a [String],
    pub(crate) pending_reason: Option<&'a str>,
}

pub(crate) fn format_card_preview(card: &Card) -> String {
    let mode = match card.mode {
        Mode::Job => "JOB",
        Mode::Cron => "CRON",
    };
    let status = match card.status {
        CardStatus::Success => "success",
        CardStatus::Error => "error",
        CardStatus::Pending => "pending",
        CardStatus::Streaming => "streaming",
    };
    let mut lines = vec![
        format!("mode: {mode}"),
        format!("input: {}", card.input),
        format!("status: {status}"),
    ];
    if let Some(label) = &card.label {
        lines.push(format!("label: {label}"));
    }
    if !card.output.is_empty() {
        lines.push(String::new());
        lines.push(card.output.clone());
    }
    lines.join("\n")
}

pub(crate) fn format_cron_preview(id: &str, label: &str, status: CronStatus) -> String {
    format!(
        "id: {id}\nstatus: {}\n{label}",
        status_view::cron_status_text(status),
    )
}

pub(crate) fn format_job_record(record: JobRecord<'_>) -> String {
    let mut lines = Vec::new();
    lines.extend(record.warnings.iter().cloned());
    if !lines.is_empty() {
        lines.push(String::new());
    }
    lines.push(record.id.to_string());
    lines.push(format!(
        "status: {}",
        status_view::job_status_text(record.status),
    ));
    if let Some(reason) = record.pending_reason {
        lines.push(format!("pending reason: {reason}"));
    }
    if let Some(start_scope) = record.start_scope {
        lines.push(format!("start scope: {start_scope}"));
    }
    if record.status.is_terminal()
        && let Some(end_scope) = record.end_scope
    {
        if record.start_scope == Some(end_scope) {
            lines.push(format!("end scope: no side effect ({end_scope})"));
        } else {
            lines.push(format!("end scope: {end_scope}"));
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_record_includes_pending_reason_and_scope_side_effects() {
        let warnings = vec!["warning: direct shell syntax".to_string()];
        let text = format_job_record(JobRecord {
            id: "J1",
            status: &ExecutionState::Succeeded,
            start_scope: Some("S@start"),
            end_scope: Some("S@end"),
            warnings: &warnings,
            pending_reason: Some("gpu: waiting GPU"),
        });

        assert!(text.contains("warning: direct shell syntax"));
        assert!(text.contains("pending reason: gpu: waiting GPU"));
        assert!(text.contains("start scope: S@start"));
        assert!(text.contains("end scope: S@end"));
    }

    #[test]
    fn job_record_marks_same_terminal_scope_as_no_side_effect() {
        let text = format_job_record(JobRecord {
            id: "J1",
            status: &ExecutionState::Succeeded,
            start_scope: Some("S@same"),
            end_scope: Some("S@same"),
            warnings: &[],
            pending_reason: None,
        });

        assert!(text.contains("end scope: no side effect (S@same)"));
    }
}
