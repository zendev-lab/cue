use std::collections::BTreeSet;

use cue_core::Mode;
use cue_core::ipc::Stream;

#[derive(Debug, Clone)]
pub(crate) struct PendingSubmission {
    pub(crate) card_index: Option<usize>,
    pub(crate) input: String,
    pub(crate) mode: Mode,
    pub(crate) warnings: Vec<String>,
    kind: PendingSubmissionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingSubmissionKind {
    User,
    Silent { description: String },
    DisplaySubscribe { id: String },
    DisplayUnsubscribe { id: String },
}

impl PendingSubmission {
    pub(crate) fn user(
        card_index: Option<usize>,
        input: String,
        mode: Mode,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            card_index,
            input,
            mode,
            warnings,
            kind: PendingSubmissionKind::User,
        }
    }

    #[cfg(test)]
    pub(crate) fn silent() -> Self {
        Self::silent_request("silent request")
    }

    pub(crate) fn silent_request(description: impl Into<String>) -> Self {
        Self {
            card_index: None,
            input: String::new(),
            mode: Mode::default(),
            warnings: Vec::new(),
            kind: PendingSubmissionKind::Silent {
                description: description.into(),
            },
        }
    }

    pub(crate) fn display_subscribe(id: String) -> Self {
        Self {
            card_index: None,
            input: String::new(),
            mode: Mode::default(),
            warnings: Vec::new(),
            kind: PendingSubmissionKind::DisplaySubscribe { id },
        }
    }

    pub(crate) fn display_unsubscribe(id: String) -> Self {
        Self {
            card_index: None,
            input: String::new(),
            mode: Mode::default(),
            warnings: Vec::new(),
            kind: PendingSubmissionKind::DisplayUnsubscribe { id },
        }
    }

    pub(crate) fn is_user_visible(&self) -> bool {
        matches!(self.kind, PendingSubmissionKind::User)
    }

    pub(crate) fn silent_description(&self) -> Option<&str> {
        match &self.kind {
            PendingSubmissionKind::Silent { description } => Some(description),
            PendingSubmissionKind::User
            | PendingSubmissionKind::DisplaySubscribe { .. }
            | PendingSubmissionKind::DisplayUnsubscribe { .. } => None,
        }
    }

    pub(crate) fn display_subscribe_id(&self) -> Option<&str> {
        match &self.kind {
            PendingSubmissionKind::DisplaySubscribe { id } => Some(id),
            PendingSubmissionKind::User
            | PendingSubmissionKind::Silent { .. }
            | PendingSubmissionKind::DisplayUnsubscribe { .. } => None,
        }
    }

    pub(crate) fn display_unsubscribe_id(&self) -> Option<&str> {
        match &self.kind {
            PendingSubmissionKind::DisplayUnsubscribe { id } => Some(id),
            PendingSubmissionKind::User
            | PendingSubmissionKind::Silent { .. }
            | PendingSubmissionKind::DisplaySubscribe { .. } => None,
        }
    }
}

pub(crate) fn pending_display_subscription_requests<'a>(
    pending: impl IntoIterator<Item = &'a PendingSubmission>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut subscribes = BTreeSet::new();
    let mut unsubscribes = BTreeSet::new();
    for pending in pending {
        match &pending.kind {
            PendingSubmissionKind::DisplaySubscribe { id } => {
                subscribes.insert(id.clone());
            }
            PendingSubmissionKind::DisplayUnsubscribe { id } => {
                unsubscribes.insert(id.clone());
            }
            PendingSubmissionKind::User | PendingSubmissionKind::Silent { .. } => {}
        }
    }
    (subscribes, unsubscribes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalCommand {
    Clear,
    Quit,
    Restart,
}

pub(crate) fn parse_local_command(input: &str) -> Option<LocalCommand> {
    let trimmed = input.trim();
    match trimmed {
        ":clear" => Some(LocalCommand::Clear),
        ":quit" | ":exit" => Some(LocalCommand::Quit),
        ":restart" => Some(LocalCommand::Restart),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DisplayRequest {
    pub(crate) stream: Stream,
    pub(crate) follow: bool,
}

pub(crate) fn display_request_from_submission(input: &str, mode: Mode) -> Option<DisplayRequest> {
    let trimmed = input.trim();
    let command = if mode == Mode::Job && !trimmed.starts_with(':') {
        return None;
    } else {
        trimmed.strip_prefix(':')?.split_whitespace().next()?
    };

    match command {
        "out" => Some(DisplayRequest {
            stream: Stream::Stdout,
            follow: false,
        }),
        "tail" => Some(DisplayRequest {
            stream: Stream::Stdout,
            follow: true,
        }),
        "err" => Some(DisplayRequest {
            stream: Stream::Stderr,
            follow: false,
        }),
        _ => None,
    }
}

pub(crate) fn operator_spacing_warnings(input: &str) -> Vec<String> {
    const OPERATORS: [&str; 4] = ["|||", "|?|", "->", "~>"];

    let mut warnings = Vec::new();
    let mut pos = 0;
    let mut in_quotes = false;

    while pos < input.len() {
        let rest = &input[pos..];
        let Some(ch) = rest.chars().next() else {
            break;
        };

        if ch == '\\' && in_quotes {
            pos += ch.len_utf8();
            if let Some(next) = input[pos..].chars().next() {
                pos += next.len_utf8();
            }
            continue;
        }

        if ch == '"' {
            in_quotes = !in_quotes;
            pos += ch.len_utf8();
            continue;
        }

        if !in_quotes && let Some(op) = OPERATORS.iter().find(|op| rest.starts_with(**op)) {
            let before_ok = input[..pos]
                .chars()
                .next_back()
                .is_none_or(char::is_whitespace);
            let after_pos = pos + op.len();
            let after_ok = input[after_pos..]
                .chars()
                .next()
                .is_none_or(char::is_whitespace);
            if !before_ok || !after_ok {
                warnings.push(format!(
                    "Warning: missing spaces around `{}`; did you mean `{}`?",
                    op,
                    spaced_operator_suggestion(input, pos, op),
                ));
            }
            pos = after_pos;
            continue;
        }

        pos += ch.len_utf8();
    }

    warnings
}

fn spaced_operator_suggestion(input: &str, pos: usize, op: &str) -> String {
    let before = input[..pos].trim_end_matches([' ', '\t']);
    let after = input[pos + op.len()..].trim_start_matches([' ', '\t']);
    format!("{before} {op} {after}")
}

pub(crate) fn precreates_card(_input: &str, _mode: Mode, _warnings: &[String]) -> bool {
    false
}

pub(crate) fn decorate_output(warnings: &[String], body: String) -> String {
    if warnings.is_empty() {
        return body;
    }
    if body.is_empty() {
        return warnings.join("\n");
    }
    format!("{}\n\n{}", warnings.join("\n"), body)
}

pub(crate) fn format_ack_message(input: &str) -> String {
    let trimmed = input.trim();
    for (prefix, verb) in [
        (":kill", "kill requested for"),
        (":cancel", "cancel requested for"),
        (":pause", "paused"),
        (":resume", "resumed"),
        (":send", "sent"),
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim();
            if !rest.is_empty() {
                return format!("{verb} {rest}");
            }
        }
    }
    "ok".to_string()
}

pub(crate) fn normalize_command_label(input: &str) -> String {
    let trimmed = input.trim();
    for prefix in [":run", ":cron"] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim();
            if !rest.is_empty() {
                return rest.to_string();
            }
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_submission_classifiers_expose_only_matching_intents() {
        let subscribe = PendingSubmission::display_subscribe("J1".into());
        let unsubscribe = PendingSubmission::display_unsubscribe("J2".into());
        let silent = PendingSubmission::silent_request("snapshot");
        let user = PendingSubmission::user(None, ":jobs".into(), Mode::Job, Vec::new());

        assert_eq!(subscribe.display_subscribe_id(), Some("J1"));
        assert_eq!(subscribe.display_unsubscribe_id(), None);
        assert_eq!(unsubscribe.display_unsubscribe_id(), Some("J2"));
        assert_eq!(silent.silent_description(), Some("snapshot"));
        assert!(user.is_user_visible());
        assert_eq!(user.silent_description(), None);
    }

    #[test]
    fn pending_subscription_requests_collect_display_intents() {
        let pending = [
            PendingSubmission::display_subscribe("J1".into()),
            PendingSubmission::display_unsubscribe("J2".into()),
            PendingSubmission::silent_request("snapshot"),
            PendingSubmission::user(None, ":jobs".into(), Mode::Job, Vec::new()),
        ];

        let (subscribes, unsubscribes) = pending_display_subscription_requests(&pending);

        assert_eq!(subscribes, BTreeSet::from(["J1".to_string()]));
        assert_eq!(unsubscribes, BTreeSet::from(["J2".to_string()]));
        assert!(pending[3].is_user_visible());
    }

    #[test]
    fn parses_local_commands() {
        assert_eq!(parse_local_command(" :clear "), Some(LocalCommand::Clear));
        assert_eq!(parse_local_command(":exit"), Some(LocalCommand::Quit));
        assert_eq!(parse_local_command(":restart"), Some(LocalCommand::Restart));
        assert_eq!(parse_local_command(":run echo hi"), None);
    }

    #[test]
    fn display_request_requires_colon_in_job_mode() {
        assert_eq!(display_request_from_submission("out J1", Mode::Job), None);
        assert_eq!(
            display_request_from_submission(":tail J1", Mode::Job),
            Some(DisplayRequest {
                stream: Stream::Stdout,
                follow: true,
            })
        );
        assert_eq!(
            display_request_from_submission(":err J1", Mode::Cron),
            Some(DisplayRequest {
                stream: Stream::Stderr,
                follow: false,
            })
        );
    }

    #[test]
    fn operator_spacing_warnings_ignore_quoted_operators() {
        assert!(operator_spacing_warnings(r#"echo "a->b""#).is_empty());
        let warnings = operator_spacing_warnings("sleep 4->ls");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("sleep 4 -> ls"));
    }

    #[test]
    fn decorate_output_combines_warnings_and_body() {
        assert_eq!(decorate_output(&[], "ok".into()), "ok");
        assert_eq!(
            decorate_output(&["warn one".into(), "warn two".into()], String::new()),
            "warn one\nwarn two"
        );
        assert_eq!(decorate_output(&["warn".into()], "ok".into()), "warn\n\nok");
    }

    #[test]
    fn ack_messages_describe_targeted_commands() {
        assert_eq!(format_ack_message(":kill J1"), "kill requested for J1");
        assert_eq!(format_ack_message(":send J1 hi"), "sent J1 hi");
        assert_eq!(format_ack_message(":jobs"), "ok");
    }

    #[test]
    fn command_labels_strip_explicit_run_and_cron_prefixes() {
        assert_eq!(normalize_command_label(":run cargo test"), "cargo test");
        assert_eq!(
            normalize_command_label(":cron every 5m cargo test"),
            "every 5m cargo test"
        );
        assert_eq!(normalize_command_label(":out J1"), ":out J1");
    }
}
