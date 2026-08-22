use cue_core::Mode;

#[derive(Debug, Clone)]
pub(crate) struct PendingSubmission {
    card_index: Option<usize>,
    input: String,
    mode: Mode,
    warnings: Vec<String>,
    kind: PendingSubmissionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingSubmissionKind {
    User,
    Retry { id: cue_core::ExecutionId },
    Silent { description: String },
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

    pub(crate) fn retry(
        card_index: Option<usize>,
        input: String,
        mode: Mode,
        warnings: Vec<String>,
        id: cue_core::ExecutionId,
    ) -> Self {
        Self {
            card_index,
            input,
            mode,
            warnings,
            kind: PendingSubmissionKind::Retry { id },
        }
    }

    pub(crate) fn retry_id(&self) -> Option<cue_core::ExecutionId> {
        match self.kind {
            PendingSubmissionKind::Retry { id } => Some(id),
            _ => None,
        }
    }

    pub(crate) fn as_user(&self) -> Self {
        let mut pending = self.clone();
        pending.kind = PendingSubmissionKind::User;
        pending
    }

    pub(crate) fn card_index(&self) -> Option<usize> {
        self.card_index
    }

    pub(crate) fn input(&self) -> &str {
        &self.input
    }

    pub(crate) fn mode(&self) -> Mode {
        self.mode
    }

    pub(crate) fn decorated_output(&self, body: String) -> String {
        decorate_output(&self.warnings, body)
    }

    pub(crate) fn ack_message(&self) -> String {
        format_ack_message(&self.input)
    }

    pub(crate) fn is_user_visible(&self) -> bool {
        matches!(
            self.kind,
            PendingSubmissionKind::User | PendingSubmissionKind::Retry { .. }
        )
    }

    pub(crate) fn silent_description(&self) -> Option<&str> {
        match &self.kind {
            PendingSubmissionKind::Silent { description } => Some(description),
            PendingSubmissionKind::User | PendingSubmissionKind::Retry { .. } => None,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_submission_classifiers_expose_only_matching_intents() {
        let silent = PendingSubmission::silent_request("snapshot");
        let user = PendingSubmission::user(None, ":jobs".into(), Mode::Job, Vec::new());

        assert_eq!(silent.silent_description(), Some("snapshot"));
        assert!(user.is_user_visible());
        assert_eq!(user.silent_description(), None);
    }

    #[test]
    fn parses_local_commands() {
        assert_eq!(parse_local_command(" :clear "), Some(LocalCommand::Clear));
        assert_eq!(parse_local_command(":exit"), Some(LocalCommand::Quit));
        assert_eq!(parse_local_command(":restart"), Some(LocalCommand::Restart));
        assert_eq!(parse_local_command(":run echo hi"), None);
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
}
