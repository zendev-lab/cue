//! Tokenizer: raw input string → `Vec<Spanned>`.
//!
//! Context-sensitive `()` handling:
//! - `(` immediately after a `Command` token → `ModeParenOpen`
//! - `(` elsewhere → `GroupOpen`

use super::token::{IdKind, ShellSyntax, ShellSyntaxKind, Span, Spanned, Token, Value};

/// Tokenizer state machine.
pub struct Tokenizer<'a> {
    input: &'a str,
    bytes: &'a [u8],
    pos: usize,
    /// The last significant (non-whitespace) token kind, for `()` disambiguation.
    last_significant: Option<TokenClass>,
    /// Whether we are currently tokenizing `:cmd(...)` mode params.
    in_mode_params: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorBoundary {
    Any,
    WhitespaceRequired,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TokenClass {
    Command,
    Other,
}

/// Tokenizer error.
#[derive(Debug, Clone, thiserror::Error)]
#[error("tokenizer error at byte {pos}: {message}")]
pub struct TokenizeError {
    pub pos: usize,
    pub message: String,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            last_significant: None,
            in_mode_params: false,
        }
    }

    /// Tokenize the entire input, returning all tokens including whitespace.
    pub fn tokenize(input: &str) -> Result<Vec<Spanned>, TokenizeError> {
        let mut t = Tokenizer::new(input);
        let mut tokens = Vec::new();
        loop {
            let spanned = t.next_token()?;
            let is_eof = spanned.token == Token::Eof;
            tokens.push(spanned);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let b = self.bytes.get(self.pos).copied()?;
        self.pos += 1;
        Some(b)
    }

    fn slice(&self, start: usize, end: usize) -> &'a str {
        &self.input[start..end]
    }

    fn next_token(&mut self) -> Result<Spanned, TokenizeError> {
        if self.pos >= self.bytes.len() {
            return Ok(Spanned {
                token: Token::Eof,
                span: Span::new(self.pos, self.pos),
            });
        }

        let start = self.pos;
        let b = self.bytes[self.pos];

        if b == b'\n' {
            self.pos += 1;
            self.last_significant = None;
            return Ok(Spanned {
                token: Token::Newline,
                span: Span::new(start, self.pos),
            });
        }

        if b == b'\r' && self.peek_at(1) == Some(b'\n') {
            self.pos += 2;
            self.last_significant = None;
            return Ok(Spanned {
                token: Token::Newline,
                span: Span::new(start, self.pos),
            });
        }

        // Whitespace
        if b == b' ' || b == b'\t' {
            self.pos += 1;
            while self.pos < self.bytes.len()
                && (self.bytes[self.pos] == b' ' || self.bytes[self.pos] == b'\t')
            {
                self.pos += 1;
            }
            return Ok(Spanned {
                token: Token::Whitespace(self.slice(start, self.pos).into()),
                span: Span::new(start, self.pos),
            });
        }

        let tok = match b {
            b':' if start == 0 || self.last_significant.is_none() => {
                self.pos += 1;
                self.last_significant = Some(TokenClass::Other);

                // Try to read command name
                let cmd_start = self.pos;
                while self.pos < self.bytes.len() && is_ident_char(self.bytes[self.pos]) {
                    self.pos += 1;
                }
                if self.pos > cmd_start {
                    let name = self.slice(cmd_start, self.pos).to_string();
                    self.last_significant = Some(TokenClass::Command);
                    // Return Colon + Command as two tokens; but for simplicity
                    // in this pass, we return just Command (colon is implicit).
                    // The parser knows all commands start with `:`.
                    return Ok(Spanned {
                        token: Token::Command(name),
                        span: Span::new(start, self.pos),
                    });
                }
                Token::Colon
            }

            b'(' => {
                self.pos += 1;
                if self.last_significant == Some(TokenClass::Command) {
                    self.in_mode_params = true;
                    self.last_significant = Some(TokenClass::Other);
                    let tok = Token::ModeParenOpen;
                    // Read mode params until `)`
                    return self.tokenize_mode_params(start, tok);
                }
                self.last_significant = Some(TokenClass::Other);
                Token::GroupOpen
            }

            b')' => {
                self.pos += 1;
                let token = if self.in_mode_params {
                    self.in_mode_params = false;
                    Token::ModeParenClose
                } else {
                    Token::GroupClose
                };
                self.last_significant = Some(TokenClass::Other);
                token
            }

            b'-' if self.peek_at(1) == Some(b'>')
                && self.operator_has_required_whitespace(start, 2) =>
            {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Token::SerialThen
            }
            b'-' if self.peek_at(1) == Some(b'>') => {
                return Err(self.missing_operator_whitespace_error(start, "->", 2));
            }

            b'~' if self.peek_at(1) == Some(b'>')
                && self.operator_has_required_whitespace(start, 2) =>
            {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Token::SerialAlways
            }
            b'~' if self.peek_at(1) == Some(b'>') => {
                return Err(self.missing_operator_whitespace_error(start, "~>", 2));
            }

            b'&' if self.peek_at(1) == Some(b'&') => {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Token::JobAnd
            }

            b'|' => self.tokenize_pipe_or_parallel()?,

            _ => {
                // Unquoted bash syntax at a token boundary becomes its own
                // token; a word only stops before it.
                if let Some(syntax) = shell_syntax_at(self.bytes, self.pos) {
                    self.pos += syntax.text.len();
                    self.last_significant = Some(TokenClass::Other);
                    Token::ShellSyntax(syntax)
                } else {
                    self.tokenize_word()?
                }
            }
        };

        if !matches!(tok, Token::Whitespace(_)) {
            self.last_significant = Some(TokenClass::Other);
        }

        Ok(Spanned {
            token: tok,
            span: Span::new(start, self.pos),
        })
    }

    /// Return the already-consumed mode-param opening paren.
    /// Subsequent calls continue in mode-param state until `ModeParenClose`.
    fn tokenize_mode_params(
        &mut self,
        paren_start: usize,
        open_tok: Token,
    ) -> Result<Spanned, TokenizeError> {
        Ok(Spanned {
            token: open_tok,
            span: Span::new(paren_start, self.pos),
        })
    }

    fn tokenize_pipe_or_parallel(&mut self) -> Result<Token, TokenizeError> {
        // Current char is `|`
        let start = self.pos;
        self.pos += 1;

        match self.peek() {
            Some(b'>') => {
                self.pos += 1;
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::PipeStdout)
            }
            Some(b'&') if self.peek_at(1) == Some(b'>') => {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::PipeAll)
            }
            Some(b'!') if self.peek_at(1) == Some(b'>') => {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::PipeStderr)
            }
            Some(b'|') if self.peek_at(1) == Some(b'|') => {
                if self.operator_has_required_whitespace(start, 3) {
                    self.pos += 2;
                    self.last_significant = Some(TokenClass::Other);
                    Ok(Token::ParallelAll)
                } else {
                    Err(self.missing_operator_whitespace_error(start, "|||", 3))
                }
            }
            Some(b'?') if self.peek_at(1) == Some(b'|') => {
                if self.operator_has_required_whitespace(start, 3) {
                    self.pos += 2;
                    self.last_significant = Some(TokenClass::Other);
                    Ok(Token::ParallelRace)
                } else {
                    Err(self.missing_operator_whitespace_error(start, "|?|", 3))
                }
            }
            Some(b'|') => {
                self.pos += 1;
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::JobOr)
            }
            _ => Err(TokenizeError {
                pos: start,
                message: "bare `|` is not a Cue pipe operator; use `|>` for stdout pipes, `|&>` for stdout+stderr, or quote `|` to pass it as an argument".into(),
            }),
        }
    }

    fn tokenize_word(&mut self) -> Result<Token, TokenizeError> {
        let start = self.pos;

        // Check for job/cron ID refs. Scopes are content-addressed hashes, not
        // numeric parser IDs.
        if let Some(kind) = self.try_id_kind() {
            let prefix_pos = self.pos;
            self.pos += 1; // skip prefix letter
            let num_start = self.pos;
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            if self.pos > num_start {
                // Make sure next char is not alphanumeric (otherwise it's a regular word)
                if (self.pos >= self.bytes.len() || !is_ident_char(self.bytes[self.pos]))
                    && let Ok(n) = self.slice(num_start, self.pos).parse::<u32>()
                {
                    self.last_significant = Some(TokenClass::Other);
                    return Ok(Token::IdRef(kind, n));
                }
            }
            // Not an ID ref, fall through to word
            self.pos = prefix_pos;
        }

        // Mode params interior tokens
        if self.in_mode_params && self.bytes[self.pos] == b'=' {
            self.pos += 1;
            self.last_significant = Some(TokenClass::Other);
            return Ok(Token::ParamEq);
        }
        if self.in_mode_params && self.bytes[self.pos] == b',' {
            self.pos += 1;
            self.last_significant = Some(TokenClass::Other);
            return Ok(Token::Comma);
        }

        // A word is the concatenation of raw runs and quoted segments, so
        // shell-style adjacency holds: `--msg="a b"` is one argument, not two,
        // and `a'b'c` is `abc`. Quotes are removed; the text they cover is
        // exempt from operator and delimiter scanning.
        let mut buf: Vec<u8> = Vec::new();

        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];

            if b == b'"' {
                self.read_double_quoted(&mut buf)?;
                continue;
            }
            if b == b'\'' {
                self.read_single_quoted(&mut buf)?;
                continue;
            }
            if self.word_run_ends_at(self.pos) {
                break;
            }
            // Stop before any Cue operator (longest-match-first).
            if let Some((token, boundary)) = starts_with_operator(self.bytes, self.pos) {
                if boundary == OperatorBoundary::WhitespaceRequired
                    && !self.operator_has_required_whitespace(
                        self.pos,
                        operator_len(self.bytes, self.pos),
                    )
                {
                    let op = token.operator_text();
                    return Err(self.missing_operator_whitespace_error(self.pos, op, op.len()));
                }
                break;
            }
            // Unquoted bash syntax becomes its own token so the parser can
            // reject it in command position while raw-text builtins ignore it.
            if shell_syntax_at(self.bytes, self.pos).is_some() {
                break;
            }

            let run_start = self.pos;
            self.pos += 1;
            while self.pos < self.bytes.len()
                && !self.word_run_ends_at(self.pos)
                && starts_with_operator(self.bytes, self.pos).is_none()
                && shell_syntax_at(self.bytes, self.pos).is_none()
            {
                self.pos += 1;
            }
            buf.extend_from_slice(&self.bytes[run_start..self.pos]);
        }

        if self.pos == start {
            // Unknown character
            self.pos += 1;
            return Err(TokenizeError {
                pos: start,
                message: format!("unexpected character '{}'", self.slice(start, self.pos)),
            });
        }

        let text = String::from_utf8(buf).map_err(|_| TokenizeError {
            pos: start,
            message: "invalid UTF-8 in word".into(),
        })?;
        self.last_significant = Some(TokenClass::Other);

        if self.in_mode_params
            && let Some(v) = try_parse_value(&text)
        {
            return Ok(Token::ParamValue(v));
        }

        Ok(Token::Word(text))
    }

    /// Tokenize a double-quoted segment, appending its literal bytes to `buf`.
    ///
    /// Escape sequences `\"`, `\\`, `\n`, `\t` are interpreted; any other
    /// backslash pair is preserved verbatim so `"\$USER"` still reaches word
    /// expansion as `\$USER`.
    fn read_double_quoted(&mut self, buf: &mut Vec<u8>) -> Result<(), TokenizeError> {
        let start = self.pos;
        self.pos += 1; // skip opening quote
        loop {
            match self.advance() {
                None => {
                    return Err(TokenizeError {
                        pos: start,
                        message: "unterminated string".into(),
                    });
                }
                Some(b'"') => break,
                Some(b'\\') => match self.advance() {
                    Some(b'"') => buf.push(b'"'),
                    Some(b'\\') => buf.push(b'\\'),
                    Some(b'n') => buf.push(b'\n'),
                    Some(b't') => buf.push(b'\t'),
                    Some(c) => {
                        buf.push(b'\\');
                        buf.push(c);
                    }
                    None => {
                        return Err(TokenizeError {
                            pos: self.pos,
                            message: "unterminated escape".into(),
                        });
                    }
                },
                Some(c) => buf.push(c),
            }
        }
        Ok(())
    }

    /// Tokenize a single-quoted segment, appending its literal bytes to `buf`.
    ///
    /// Single quotes capture everything literally until the closing `'`;
    /// unlike double quotes there are no escape sequences.
    fn read_single_quoted(&mut self, buf: &mut Vec<u8>) -> Result<(), TokenizeError> {
        let start = self.pos;
        self.pos += 1; // skip opening quote
        loop {
            match self.advance() {
                None => {
                    return Err(TokenizeError {
                        pos: start,
                        message: "unterminated single-quoted string".into(),
                    });
                }
                Some(b'\'') => break,
                Some(c) => buf.push(c),
            }
        }
        Ok(())
    }

    /// Whether an unquoted word run must stop at `pos`.
    ///
    /// Quote bytes end a raw run so the outer word loop can consume the quoted
    /// segment and continue appending to the same word.
    fn word_run_ends_at(&self, pos: usize) -> bool {
        let b = self.bytes[pos];
        if b == b'"' || b == b'\'' {
            return true;
        }
        if is_delimiter(b) {
            return true;
        }
        self.in_mode_params && (b == b'=' || b == b',')
    }

    fn try_id_kind(&self) -> Option<IdKind> {
        match self.peek()? {
            b'J' if self.peek_at(1).is_some_and(|b| b.is_ascii_digit()) => Some(IdKind::Job),
            b'C' if self.peek_at(1).is_some_and(|b| b.is_ascii_digit()) => Some(IdKind::Cron),
            _ => None,
        }
    }

    fn operator_has_required_whitespace(&self, start: usize, len: usize) -> bool {
        is_operator_boundary_before(self.bytes, start)
            && is_operator_boundary_after(self.bytes, start + len)
    }

    fn missing_operator_whitespace_error(
        &self,
        start: usize,
        operator: &str,
        len: usize,
    ) -> TokenizeError {
        let end = (start + len).min(self.bytes.len());
        TokenizeError {
            pos: start,
            message: format!(
                "cue chain operator `{operator}` must be surrounded by whitespace; quote it to pass it as an argument (saw `{}`)",
                self.slice(start, end)
            ),
        }
    }
}

/// Check whether `bytes[pos..]` starts with a Cue operator.
/// Operators are checked longest-match-first so longer operators are not split.
fn starts_with_operator(bytes: &[u8], pos: usize) -> Option<(Token, OperatorBoundary)> {
    let tail = &bytes[pos..];
    if tail.len() < 2 {
        return None;
    }
    // longest match first
    if tail.starts_with(b"|&>") {
        return Some((Token::PipeAll, OperatorBoundary::Any));
    }
    if tail.starts_with(b"|!>") {
        return Some((Token::PipeStderr, OperatorBoundary::Any));
    }
    if tail.starts_with(b"|||") {
        return Some((Token::ParallelAll, OperatorBoundary::WhitespaceRequired));
    }
    if tail.starts_with(b"|?|") {
        return Some((Token::ParallelRace, OperatorBoundary::WhitespaceRequired));
    }
    if tail.starts_with(b"->") {
        return Some((Token::SerialThen, OperatorBoundary::WhitespaceRequired));
    }
    if tail.starts_with(b"~>") {
        return Some((Token::SerialAlways, OperatorBoundary::WhitespaceRequired));
    }
    if tail.starts_with(b"|>") {
        return Some((Token::PipeStdout, OperatorBoundary::Any));
    }
    if tail.starts_with(b"&&") {
        return Some((Token::JobAnd, OperatorBoundary::Any));
    }
    if tail.starts_with(b"||") {
        return Some((Token::JobOr, OperatorBoundary::Any));
    }
    None
}

fn operator_len(bytes: &[u8], pos: usize) -> usize {
    let tail = &bytes[pos..];
    for op in [
        b"|&>".as_slice(),
        b"|!>",
        b"|||",
        b"|?|",
        b"->",
        b"~>",
        b"|>",
        b"&&",
        b"||",
    ] {
        if tail.starts_with(op) {
            return op.len();
        }
    }
    0
}

fn is_operator_boundary_before(bytes: &[u8], start: usize) -> bool {
    start == 0 || matches!(bytes[start - 1], b' ' | b'\t' | b'\n' | b'\r')
}

fn is_operator_boundary_after(bytes: &[u8], end: usize) -> bool {
    end >= bytes.len() || matches!(bytes[end], b' ' | b'\t' | b'\n' | b'\r')
}

/// Detect unquoted bash syntax at `pos`.
///
/// Before this check these bytes were neither delimiters nor operators, so they
/// became ordinary argv elements: `wc -l > out.txt` ran `wc` with the literal
/// arguments `>` and `out.txt`. Silently passing them on is the worst outcome
/// for a caller that assumed bash, so they are tokenized separately and then
/// rejected with the Cue equivalent.
fn shell_syntax_at(bytes: &[u8], pos: usize) -> Option<ShellSyntax> {
    use ShellSyntaxKind::{CommandSubstitution, Redirect, Semicolon};

    let tail = &bytes[pos..];
    // Longest match first so `2>>` is not reported as `2>`.
    for (text, kind) in [
        ("$(", CommandSubstitution),
        ("2>>", Redirect),
        ("&>>", Redirect),
        ("1>>", Redirect),
        ("1>", Redirect),
        ("2>", Redirect),
        ("&>", Redirect),
        (">>", Redirect),
        (">&", Redirect),
        ("<<", Redirect),
        (">", Redirect),
        ("<", Redirect),
        (";", Semicolon),
        ("`", CommandSubstitution),
    ] {
        if tail.starts_with(text.as_bytes()) {
            return Some(ShellSyntax { kind, text });
        }
    }
    None
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_delimiter(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'(' | b')' | b'|')
    // Note: quote bytes are NOT delimiters.  `word_run_ends_at` stops a raw run
    // at a quote so the word loop can consume the quoted segment and keep
    // appending to the same word; that is what makes `--msg="a b"` one argument.
    // Note: Comma is NOT a general delimiter.  It is part of words outside
    // mode-params context.  Inside mode-params `word_run_ends_at` explicitly
    // stops at `,` so key=val pairs are split correctly.
    // Note: `-` and `~` are NOT delimiters here.
    // The main tokenize loop handles `->` and `~>` as operators before
    // falling through to word tokenization, so `-` inside words (e.g. `--release`)
    // is correctly consumed as part of the word.
}

/// Try to parse a word as a typed value (for mode params).
fn try_parse_value(s: &str) -> Option<Value> {
    if s == "true" {
        return Some(Value::Bool(true));
    }
    if s == "false" {
        return Some(Value::Bool(false));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(input: &str) -> Vec<Token> {
        Tokenizer::tokenize(input)
            .unwrap()
            .into_iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .map(|s| s.token)
            .collect()
    }

    #[test]
    fn simple_command() {
        let toks = tokens(":run cargo test");
        assert_eq!(
            toks,
            vec![
                Token::Command("run".into()),
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn newline_is_tokenized() {
        let toks = tokens("echo hi\npwd");
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word("hi".into()),
                Token::Newline,
                Token::Word("pwd".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn command_with_mode_params() {
        let toks = tokens(":run(pty=false) cargo test");
        assert_eq!(
            toks,
            vec![
                Token::Command("run".into()),
                Token::ModeParenOpen,
                Token::Word("pty".into()),
                Token::ParamEq,
                Token::ParamValue(Value::Bool(false)),
                Token::ModeParenClose,
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_operators() {
        let toks = tokens("a -> b ~> c ||| d |?| e");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::SerialThen,
                Token::Word("b".into()),
                Token::SerialAlways,
                Token::Word("c".into()),
                Token::ParallelAll,
                Token::Word("d".into()),
                Token::ParallelRace,
                Token::Word("e".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn env_assignment_is_single_word_outside_mode_params() {
        let toks = tokens(":env set FOO=bar");
        assert_eq!(
            toks,
            vec![
                Token::Command("env".into()),
                Token::Word("set".into()),
                Token::Word("FOO=bar".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn pipe_operators() {
        let toks = tokens("a |> b |&> c |!> d");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::PipeStdout,
                Token::Word("b".into()),
                Token::PipeAll,
                Token::Word("c".into()),
                Token::PipeStderr,
                Token::Word("d".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn bare_pipe_is_rejected_instead_of_becoming_a_word() {
        for input in ["a | b", "a|b"] {
            let err = Tokenizer::tokenize(input).unwrap_err();
            assert_eq!(err.pos, input.find('|').expect("test input has pipe"));
            assert!(
                err.message.contains("bare `|` is not a Cue pipe operator"),
                "unexpected error for {input}: {err}"
            );
            assert!(
                err.message.contains("use `|>`"),
                "error should point to Cue pipe syntax for {input}: {err}"
            );
        }
    }

    #[test]
    fn id_refs() {
        let toks = tokens(":kill J1");
        assert_eq!(
            toks,
            vec![
                Token::Command("kill".into()),
                Token::IdRef(IdKind::Job, 1),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn numeric_scope_labels_stay_words() {
        let toks = tokens("echo S1");
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word("S1".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn oversized_id_refs_stay_words_instead_of_wrapping_to_zero() {
        let toks = tokens("echo J4294967296");
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word("J4294967296".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn grouping_parens() {
        let toks = tokens("(a -> b) ||| c");
        assert_eq!(
            toks,
            vec![
                Token::GroupOpen,
                Token::Word("a".into()),
                Token::SerialThen,
                Token::Word("b".into()),
                Token::GroupClose,
                Token::ParallelAll,
                Token::Word("c".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn bare_input() {
        let toks = tokens("cargo test --release");
        assert_eq!(
            toks,
            vec![
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Word("--release".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn bare_numeric_words_stay_words() {
        let toks = tokens("sleep 4");
        assert_eq!(
            toks,
            vec![
                Token::Word("sleep".into()),
                Token::Word("4".into()),
                Token::Eof,
            ]
        );

        let toks = tokens("sleep 4s");
        assert_eq!(
            toks,
            vec![
                Token::Word("sleep".into()),
                Token::Word("4s".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn non_leading_colons_stay_in_words() {
        let toks = tokens(":run tr [:upper:] [:lower:] https://example.com at 14:30");
        assert_eq!(
            toks,
            vec![
                Token::Command("run".into()),
                Token::Word("tr".into()),
                Token::Word("[:upper:]".into()),
                Token::Word("[:lower:]".into()),
                Token::Word("https://example.com".into()),
                Token::Word("at".into()),
                Token::Word("14:30".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn complex_chain_with_pipes() {
        let toks = tokens("cargo build |> grep error -> cargo test ||| cargo clippy");
        assert_eq!(
            toks,
            vec![
                Token::Word("cargo".into()),
                Token::Word("build".into()),
                Token::PipeStdout,
                Token::Word("grep".into()),
                Token::Word("error".into()),
                Token::SerialThen,
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::ParallelAll,
                Token::Word("cargo".into()),
                Token::Word("clippy".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_with_dash_args() {
        // `-A` should be a word, not confused with `->`
        let toks = tokens("git add -A -> git commit -m \"fix\"");
        assert_eq!(
            toks,
            vec![
                Token::Word("git".into()),
                Token::Word("add".into()),
                Token::Word("-A".into()),
                Token::SerialThen,
                Token::Word("git".into()),
                Token::Word("commit".into()),
                Token::Word("-m".into()),
                Token::Word("fix".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_with_colon_in_quoted_arg() {
        // `:wrap` inside quotes should be a word, not a command
        let toks = tokens("echo \":wrap on\" -> echo done");
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word(":wrap on".into()),
                Token::SerialThen,
                Token::Word("echo".into()),
                Token::Word("done".into()),
                Token::Eof,
            ]
        );
    }

    /// Cue chain operators require whitespace when unquoted.
    #[test]
    fn chain_operators_without_whitespace_error() {
        for input in [
            "a-> b",
            "a ->b",
            "a~> b",
            "a ~>b",
            "a||| b",
            "a |||b",
            "a|?| b",
            "a |?|b",
            "cmd -A-> cmd2",
        ] {
            let err = Tokenizer::tokenize(input).unwrap_err();
            assert!(
                err.message.contains("must be surrounded by whitespace"),
                "unexpected error for {input}: {err}"
            );
        }
    }

    #[test]
    fn chain_operators_allow_newline_boundaries() {
        let cases: &[(&str, Token)] = &[
            ("a\n->\nb", Token::SerialThen),
            ("a\n~>\nb", Token::SerialAlways),
            ("a\n|||\nb", Token::ParallelAll),
            ("a\n|?|\nb", Token::ParallelRace),
        ];
        for (input, expected_op) in cases {
            let toks = tokens(input);
            assert_eq!(
                toks,
                vec![
                    Token::Word("a".into()),
                    Token::Newline,
                    expected_op.clone(),
                    Token::Newline,
                    Token::Word("b".into()),
                    Token::Eof,
                ],
                "failed for input: {input}"
            );
        }
    }

    #[test]
    fn quoted_chain_operators_are_words_without_whitespace() {
        let toks = tokens("echo 'a->b' \"c~>d\" 'e|||f' \"g|?|h\"");
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word("a->b".into()),
                Token::Word("c~>d".into()),
                Token::Word("e|||f".into()),
                Token::Word("g|?|h".into()),
                Token::Eof,
            ]
        );
    }

    /// Job and pipe operators still work without extra whitespace.
    #[test]
    fn all_operators_inside_words() {
        let cases: &[(&str, Token)] = &[
            ("a|>b", Token::PipeStdout),
            ("a|&>b", Token::PipeAll),
            ("a|!>b", Token::PipeStderr),
            ("a&&b", Token::JobAnd),
            ("a||b", Token::JobOr),
        ];
        for (input, expected_op) in cases {
            let toks = tokens(input);
            assert_eq!(
                toks,
                vec![
                    Token::Word("a".into()),
                    expected_op.clone(),
                    Token::Word("b".into()),
                    Token::Eof,
                ],
                "failed for input: {input}"
            );
        }
    }

    #[test]
    fn emoji_in_words() {
        let tokens = Tokenizer::tokenize("echo 🎉").unwrap();
        let filtered: Vec<_> = tokens
            .iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .collect();
        assert_eq!(filtered[0].token, Token::Word("echo".into()));
        assert_eq!(filtered[1].token, Token::Word("🎉".into()));

        // Multi-emoji
        let tokens = Tokenizer::tokenize("echo 🎉✅🚀").unwrap();
        let filtered: Vec<_> = tokens
            .iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .collect();
        assert_eq!(filtered[1].token, Token::Word("🎉✅🚀".into()));

        // Single-quoted emoji
        let tokens = Tokenizer::tokenize("echo '📝'").unwrap();
        let filtered: Vec<_> = tokens
            .iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .collect();
        assert_eq!(filtered[1].token, Token::Word("📝".into()));

        // Double-quoted emoji
        let tokens = Tokenizer::tokenize("echo \"📝\"").unwrap();
        let filtered: Vec<_> = tokens
            .iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .collect();
        assert_eq!(filtered[1].token, Token::Word("📝".into()));

        // Emoji in mode params
        let tokens = Tokenizer::tokenize(":run(desc=🎉) cargo test").unwrap();
        let filtered: Vec<_> = tokens
            .iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .collect();
        assert_eq!(filtered[0].token, Token::Command("run".into()));
        assert_eq!(filtered[1].token, Token::ModeParenOpen);
        // ModeParam value with emoji (token is Word, parser converts to Value::Str)
        let has_emoji_word = filtered
            .iter()
            .any(|s| matches!(&s.token, Token::Word(w) if w == "🎉"));
        assert!(
            has_emoji_word,
            "emoji should survive mode param tokenization"
        );
    }

    #[test]
    fn quotes_join_adjacent_runs_into_one_word() {
        // A quote is a lexical boundary, not an argument boundary: shell-style
        // adjacency must hold so `--msg="a b"` stays a single argv element.
        let toks = tokens(r#"cmd --msg="a b" "a b"c a'b'c pre"mid"post"#);
        assert_eq!(
            toks,
            vec![
                Token::Word("cmd".into()),
                Token::Word("--msg=a b".into()),
                Token::Word("a bc".into()),
                Token::Word("abc".into()),
                Token::Word("premidpost".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn quoted_segments_hide_operators_and_delimiters() {
        // Text inside quotes is exempt from operator and delimiter scanning even
        // when the word starts unquoted.
        let toks = tokens(r#"cmd pre'a -> b'post x"y ||| z" 'a|b'"#);
        assert_eq!(
            toks,
            vec![
                Token::Word("cmd".into()),
                Token::Word("prea -> bpost".into()),
                Token::Word("xy ||| z".into()),
                Token::Word("a|b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn mid_word_quotes_do_not_start_a_command() {
        // `:` only introduces a builtin at the start of input, and a quoted
        // segment must never be reinterpreted as one.
        let toks = tokens(r#"echo a":wrap on"b"#);
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word("a:wrap onb".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_mid_word_quotes_are_rejected() {
        for input in [r#"echo a"b"#, "echo a'b"] {
            let err = Tokenizer::tokenize(input).unwrap_err();
            assert!(
                err.message.contains("unterminated"),
                "unexpected error for {input}: {err}"
            );
        }
    }

    #[test]
    fn mode_param_values_support_quoted_segments() {
        let toks = tokens(r#":run(cwd="/tmp/a b") cargo test"#);
        assert_eq!(
            toks,
            vec![
                Token::Command("run".into()),
                Token::ModeParenOpen,
                Token::Word("cwd".into()),
                Token::ParamEq,
                Token::Word("/tmp/a b".into()),
                Token::ModeParenClose,
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn unquoted_shell_syntax_becomes_its_own_token() {
        // These bytes used to be swallowed into words and handed to exec as
        // literal argv elements. They must now be visible to the parser.
        let toks = tokens("wc -l > out.txt");
        assert_eq!(
            toks,
            vec![
                Token::Word("wc".into()),
                Token::Word("-l".into()),
                Token::ShellSyntax(ShellSyntax {
                    kind: ShellSyntaxKind::Redirect,
                    text: ">",
                }),
                Token::Word("out.txt".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn shell_syntax_is_matched_longest_first() {
        let cases: &[(&str, &str, ShellSyntaxKind)] = &[
            ("a 2>&1", "2>", ShellSyntaxKind::Redirect),
            ("a 2>>log", "2>>", ShellSyntaxKind::Redirect),
            ("a &>>log", "&>>", ShellSyntaxKind::Redirect),
            ("a >>log", ">>", ShellSyntaxKind::Redirect),
            ("a <in", "<", ShellSyntaxKind::Redirect),
            ("a <<EOF", "<<", ShellSyntaxKind::Redirect),
            ("a; b", ";", ShellSyntaxKind::Semicolon),
            ("a $(b)", "$(", ShellSyntaxKind::CommandSubstitution),
            ("a `b`", "`", ShellSyntaxKind::CommandSubstitution),
        ];
        for (input, text, kind) in cases {
            let toks = tokens(input);
            assert!(
                toks.contains(&Token::ShellSyntax(ShellSyntax { kind: *kind, text })),
                "expected {text} token for {input}, got {toks:?}"
            );
        }
    }

    #[test]
    fn shell_syntax_ends_an_adjacent_word() {
        // `a>b` must not stay one opaque word; splitting it is what lets the
        // parser explain the failure.
        let toks = tokens("a>b");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::ShellSyntax(ShellSyntax {
                    kind: ShellSyntaxKind::Redirect,
                    text: ">",
                }),
                Token::Word("b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn quoted_shell_syntax_stays_literal_data() {
        let toks = tokens(r#"echo 'a; b' "c > d" 'e $(f)' "g `h`""#);
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word("a; b".into()),
                Token::Word("c > d".into()),
                Token::Word("e $(f)".into()),
                Token::Word("g `h`".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn comma_in_command_args_is_word() {
        // Regression: commas outside mode-params should be part of the word.
        let toks = tokens("gh search prs --json number,title,author");
        assert_eq!(
            toks,
            vec![
                Token::Word("gh".into()),
                Token::Word("search".into()),
                Token::Word("prs".into()),
                Token::Word("--json".into()),
                Token::Word("number,title,author".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn comma_in_mode_params_still_separates() {
        // Inside mode params, commas should still separate key=val pairs.
        let toks = tokens(":run(cwd=/tmp,pty=false) cargo test");
        assert_eq!(
            toks,
            vec![
                Token::Command("run".into()),
                Token::ModeParenOpen,
                Token::Word("cwd".into()),
                Token::ParamEq,
                Token::Word("/tmp".into()),
                Token::Comma,
                Token::Word("pty".into()),
                Token::ParamEq,
                Token::ParamValue(Value::Bool(false)),
                Token::ModeParenClose,
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Eof,
            ]
        );
    }
}
