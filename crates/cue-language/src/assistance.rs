use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionItem {
    pub label: String,
    pub insert_text: String,
    pub kind: CompletionKind,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionKind {
    Command,
    Param,
    Id,
    Path,
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HighlightSpan {
    pub start: usize,
    pub end: usize,
    pub kind: HighlightKind,
}

impl HighlightSpan {
    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HighlightKind {
    CommandPrefix,
    CommandName,
    ModeParam,
    Operator,
    IdRef,
    Word,
    String,
    Number,
    Error,
}

use crate::{
    Token, Tokenizer,
    command_spec::{command_names, command_spec, mode_param_specs_for_command},
};

/// Complete language-owned command and mode-parameter syntax.
///
/// Filesystem and live entity candidates are supplied by the frontend through
/// [`crate::completion_candidates`]; they do not require daemon RPC.
pub fn complete_input(input: &str, cursor: usize) -> Vec<CompletionItem> {
    let prefix = prefix_before_cursor(input, cursor).trim_start();

    if let Some((command, param_prefix)) = mode_param_key_prefix(prefix) {
        return mode_param_specs_for_command(command)
            .filter(|param| param.name.starts_with(param_prefix))
            .map(|param| CompletionItem {
                label: param.name.into(),
                insert_text: format!("{}={}", param.name, param.value_hint),
                kind: CompletionKind::Param,
                detail: Some(param.detail.into()),
            })
            .collect();
    }

    if let Some(command_prefix) = prefix.strip_prefix(':') {
        let word = command_prefix
            .rsplit_once(char::is_whitespace)
            .map(|(_, word)| word)
            .unwrap_or(command_prefix);
        return command_names()
            .filter_map(command_spec)
            .filter(|spec| spec.name.starts_with(word))
            .map(|spec| CompletionItem {
                label: format!(":{}", spec.name),
                insert_text: format!(":{}", spec.name),
                kind: CompletionKind::Command,
                detail: Some(spec.detail.into()),
            })
            .collect();
    }

    Vec::new()
}

/// Produce syntax spans entirely from the local tokenizer.
pub fn highlight_input(input: &str) -> Vec<HighlightSpan> {
    match Tokenizer::tokenize(input) {
        Ok(tokens) => tokens
            .into_iter()
            .filter_map(|spanned| {
                let kind = match spanned.token {
                    Token::Command(_) => HighlightKind::CommandName,
                    Token::ModeParenOpen
                    | Token::ModeParenClose
                    | Token::ParamEq
                    | Token::ParamValue(_)
                    | Token::Comma => HighlightKind::ModeParam,
                    Token::SerialThen
                    | Token::SerialAlways
                    | Token::ParallelAll
                    | Token::ParallelRace
                    | Token::JobAnd
                    | Token::JobOr
                    | Token::PipeStdout
                    | Token::PipeAll
                    | Token::PipeStderr => HighlightKind::Operator,
                    Token::IdRef(_, _) => HighlightKind::IdRef,
                    Token::Word(_) => HighlightKind::Word,
                    Token::ShellSyntax(_) => HighlightKind::Error,
                    Token::Colon => HighlightKind::CommandPrefix,
                    Token::GroupOpen | Token::GroupClose => HighlightKind::Word,
                    Token::Whitespace(_) | Token::Newline | Token::Eof => return None,
                };
                Some(HighlightSpan {
                    start: spanned.span.start,
                    end: spanned.span.end,
                    kind,
                })
            })
            .collect(),
        Err(error) => vec![HighlightSpan {
            start: error.pos,
            end: error.pos.saturating_add(1).min(input.len()),
            kind: HighlightKind::Error,
        }],
    }
}

fn prefix_before_cursor(input: &str, cursor: usize) -> &str {
    let mut cursor = cursor.min(input.len());
    while !input.is_char_boundary(cursor) {
        cursor -= 1;
    }
    &input[..cursor]
}

fn mode_param_key_prefix(prefix: &str) -> Option<(&str, &str)> {
    let open = prefix.rfind('(')?;
    let command = prefix[..open].strip_prefix(':')?;
    let command = command.split_whitespace().next().unwrap_or(command);
    if !command_spec(command)?.accepts_mode_params() {
        return None;
    }
    let params = &prefix[open + 1..];
    if params.contains(')') {
        return None;
    }
    let current = params
        .rsplit_once([',', ' ', '\t'])
        .map(|(_, current)| current)
        .unwrap_or(params);
    if current.contains('=') {
        return None;
    }
    Some((command, current))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_uses_shared_command_specs() {
        let items = complete_input(":ta", 3);
        assert!(items.iter().any(|item| item.label == ":tail"));
    }

    #[test]
    fn completion_clamps_cursor_to_utf8_boundary() {
        let input = ":r💖un";
        let cursor_inside_heart = ":r".len() + 1;

        assert_eq!(prefix_before_cursor(input, cursor_inside_heart), ":r");
        let items = complete_input(input, cursor_inside_heart);
        assert!(items.iter().any(|item| item.label == ":run"));
    }

    #[test]
    fn completion_uses_shared_mode_param_specs() {
        let items = complete_input(":run(p", 6);
        assert!(items.iter().any(|item| item.label == "pty"));
        assert!(!items.iter().any(|item| item.label == "retry"));

        let cron_items = complete_input(":cron(p", 7);
        assert!(!cron_items.iter().any(|item| item.label == "pty"));
    }

    #[test]
    fn highlight_tokenizes_command_and_operator_spans() {
        let spans = highlight_input(":run cargo test -> :jobs");
        assert!(
            spans
                .iter()
                .any(|span| span.kind == HighlightKind::CommandName)
        );
        assert!(
            spans
                .iter()
                .any(|span| span.kind == HighlightKind::Operator)
        );
    }
}
