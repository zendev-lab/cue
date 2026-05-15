//! Recursive descent parser: Vec<Token> → Ast.
//!
//! Grammar (EBNF):
//! ```ebnf
//! input       = command | bare_input
//! command     = ":" cmd_name mode_params? argument
//! bare_input  = chain
//!
//! chain       = parallel (serial_op parallel)*
//! parallel    = pipeline (parallel_op pipeline)*
//! pipeline    = atom (pipe_op atom)*
//! atom        = "(" chain ")" | word+
//!
//! serial_op   = "->" | "~>"
//! parallel_op = "||" | "||?"
//! pipe_op     = "|>" | "|&>" | "|!>"
//! ```

use cue_core::pipeline::{ParallelOp, PipeOp, SerialOp};

use super::ast::{Argument, Ast, ChainNode, PipeSegment, Pipeline};
use super::token::{Span, Spanned, Token, Value};
use super::tokenizer::Tokenizer;

/// Parser error.
#[derive(Debug, Clone, thiserror::Error)]
#[error("parse error at byte {span:?}: {message}")]
pub struct ParseError {
    pub span: Span,
    pub message: String,
    pub kind: ParseErrorKind,
    pub suggestions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseErrorKind {
    UnknownCommand,
    InvalidModeParam,
    UnexpectedToken,
    MissingArgument,
    InvalidIdRef,
    UnmatchedParen,
    InvalidOperator,
    InvalidCronSchedule,
}

/// Recursive descent parser.
pub struct Parser<'a> {
    tokens: Vec<Spanned>,
    /// Current position (index into tokens, skipping whitespace).
    pos: usize,
    /// Input length for EOF spans.
    input_len: usize,
    /// Original input string for raw-text extraction.
    input: &'a str,
}

impl<'a> Parser<'a> {
    /// Parse a raw input string into an AST.
    pub fn parse(input: &'a str) -> Result<Ast, ParseError> {
        let all_tokens = Tokenizer::tokenize(input).map_err(|e| ParseError {
            span: Span::new(e.pos, e.pos + 1),
            message: e.message,
            kind: ParseErrorKind::UnexpectedToken,
            suggestions: vec![],
        })?;

        // Filter whitespace for the parser (keep spans intact).
        let tokens: Vec<Spanned> = all_tokens
            .into_iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .collect();

        let mut parser = Parser {
            tokens,
            pos: 0,
            input_len: input.len(),
            input,
        };
        parser.parse_input()
    }

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .map(|s| &s.token)
            .unwrap_or(&Token::Eof)
    }

    fn peek_span(&self) -> Span {
        self.tokens
            .get(self.pos)
            .map(|s| s.span)
            .unwrap_or(Span::new(self.input_len, self.input_len))
    }

    fn advance(&mut self) -> &Spanned {
        let s = &self.tokens[self.pos];
        self.pos += 1;
        s
    }

    fn expect(&mut self, expected: &Token) -> Result<&Spanned, ParseError> {
        if self.peek() == expected {
            Ok(self.advance())
        } else {
            Err(ParseError {
                span: self.peek_span(),
                message: format!("expected {expected}, found {}", self.peek()),
                kind: ParseErrorKind::UnexpectedToken,
                suggestions: vec![],
            })
        }
    }

    fn at_end(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    fn parse_input(&mut self) -> Result<Ast, ParseError> {
        let span_start = self.peek_span().start;

        if let Token::Command(_) = self.peek() {
            return self.parse_command(span_start);
        }

        // Bare input
        if self.at_end() {
            return Ok(Ast::BareInput {
                argument: Argument::Empty,
                span: Span::new(span_start, span_start),
            });
        }

        let chain = self.parse_chain()?;
        let span_end = self.peek_span().end;
        Ok(Ast::BareInput {
            argument: Argument::Chain(chain),
            span: Span::new(span_start, span_end),
        })
    }

    fn parse_command(&mut self, span_start: usize) -> Result<Ast, ParseError> {
        let name = match self.advance().token.clone() {
            Token::Command(n) => n,
            _ => unreachable!(),
        };

        // Mode params?
        let mode_params = if matches!(self.peek(), Token::ModeParenOpen) {
            self.advance(); // consume ModeParenOpen
            self.parse_mode_params()?
        } else {
            vec![]
        };

        // Parse argument based on command classification
        let argument = self.parse_argument_for_command(&name)?;
        let span_end = self.peek_span().start;

        Ok(Ast::Command {
            name,
            mode_params,
            argument,
            span: Span::new(span_start, span_end),
        })
    }

    fn parse_mode_params(&mut self) -> Result<Vec<(String, Value)>, ParseError> {
        let mut params = vec![];
        loop {
            match self.peek() {
                Token::GroupClose => {
                    self.advance(); // consume )
                    break;
                }
                Token::Eof => {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: "unterminated mode params".into(),
                        kind: ParseErrorKind::UnmatchedParen,
                        suggestions: vec![],
                    });
                }
                Token::Comma => {
                    self.advance();
                    continue;
                }
                _ => {
                    // Expect key=value
                    let key = match self.peek().clone() {
                        Token::Word(k) => {
                            self.advance();
                            k
                        }
                        _ => {
                            return Err(ParseError {
                                span: self.peek_span(),
                                message: format!("expected parameter name, found {}", self.peek()),
                                kind: ParseErrorKind::InvalidModeParam,
                                suggestions: vec![],
                            });
                        }
                    };
                    self.expect(&Token::ParamEq)?;
                    let value = match self.peek().clone() {
                        Token::ParamValue(v) => {
                            self.advance();
                            v
                        }
                        Token::Word(s) => {
                            self.advance();
                            Value::Str(s)
                        }
                        _ => {
                            return Err(ParseError {
                                span: self.peek_span(),
                                message: format!("expected parameter value, found {}", self.peek()),
                                kind: ParseErrorKind::InvalidModeParam,
                                suggestions: vec![],
                            });
                        }
                    };
                    params.push((key, value));
                }
            }
        }
        Ok(params)
    }

    /// Determine argument type based on command name.
    fn parse_argument_for_command(&mut self, name: &str) -> Result<Argument, ParseError> {
        match name {
            // Chain argument
            "run" => {
                if self.at_end() {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: "`:run` requires a command".into(),
                        kind: ParseErrorKind::MissingArgument,
                        suggestions: vec![":run cargo test".into()],
                    });
                }
                Ok(Argument::Chain(self.parse_chain()?))
            }

            // Tail: IdRef + optional byte count
            "tail" => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    self.advance();
                    let bytes = match self.peek().clone() {
                        Token::Word(w) => {
                            if let Ok(b) = w.parse::<usize>() {
                                self.advance();
                                Some(b)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    Ok(Argument::TailRef(kind, n, bytes))
                } else {
                    Err(ParseError {
                        span: self.peek_span(),
                        message: ":tail requires an ID (e.g. J1)".into(),
                        kind: ParseErrorKind::InvalidIdRef,
                        suggestions: vec![":tail J1".into(), ":tail J1 1024".into()],
                    })
                }
            }

            // IdRef argument
            "kill" | "retry" | "out" | "err" | "fg" | "wait" | "cancel" | "pause" | "resume" => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    self.advance();
                    Ok(Argument::IdRef(kind, n))
                } else {
                    Err(ParseError {
                        span: self.peek_span(),
                        message: format!(":{name} requires an ID (e.g. J1, C1)"),
                        kind: ParseErrorKind::InvalidIdRef,
                        suggestions: vec![format!(":{name} J1")],
                    })
                }
            }

            // Text argument (use raw text to preserve operator chars like ->)
            "send" => {
                let text = self.consume_remaining_raw_text();
                if text.is_empty() {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: match name {
                            "send" => "`:send` requires a target and input".into(),
                            _ => format!(":{name} requires an argument"),
                        },
                        kind: ParseErrorKind::MissingArgument,
                        suggestions: vec![],
                    });
                }
                Ok(Argument::Text(text))
            }

            // Cron expression is parsed later by the resolver so richer schedule
            // forms can share the same splitting logic as bare CRON mode.
            "cron" => {
                if self.at_end() {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: "`:cron` requires a schedule expression".into(),
                        kind: ParseErrorKind::MissingArgument,
                        suggestions: vec![":cron every 5m cargo test".into()],
                    });
                }
                Ok(Argument::Chain(self.parse_chain()?))
            }

            // Log: optional IdRef
            "log" => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    self.advance();
                    Ok(Argument::IdRef(kind, n))
                } else {
                    Ok(Argument::Empty)
                }
            }

            // Empty or text subcommands
            "jobs" | "crons" | "scopes" | "clear" | "quit" | "exit" => Ok(Argument::Empty),

            "help" | "config" | "env" | "scope" | "cd" | "wrap" => {
                let text = self.consume_remaining_text();
                if text.is_empty() {
                    Ok(Argument::Empty)
                } else {
                    Ok(Argument::Text(text))
                }
            }

            _ => Err(ParseError {
                span: self.peek_span(),
                message: format!("unknown command `:{name}`"),
                kind: ParseErrorKind::UnknownCommand,
                suggestions: suggest_command(name),
            }),
        }
    }

    /// Consume all remaining tokens as a single text string.
    fn consume_remaining_text(&mut self) -> String {
        let mut parts = vec![];
        while !self.at_end() {
            parts.push(self.advance().token.to_string());
        }
        parts.join(" ")
    }

    /// Consume remaining input as raw text (preserving original characters).
    ///
    /// Uses token spans to extract from the original input, so operator
    /// characters like `->` inside raw-text arguments are preserved as-is.
    fn consume_remaining_raw_text(&mut self) -> String {
        if self.at_end() {
            return String::new();
        }
        let start = self.tokens[self.pos].span.start;
        // Skip all remaining tokens to advance position.
        let end_pos = self
            .tokens
            .last()
            .map(|s| s.span.end)
            .unwrap_or(self.input_len);
        while !self.at_end() {
            self.advance();
        }
        self.input[start..end_pos].to_string()
    }

    // ── Chain grammar (recursive descent) ──

    /// chain = parallel (serial_op parallel)*
    fn parse_chain(&mut self) -> Result<ChainNode, ParseError> {
        let mut left = self.parse_parallel()?;
        loop {
            let op = match self.peek() {
                Token::SerialThen => SerialOp::Then,
                Token::SerialAlways => SerialOp::Always,
                _ => break,
            };
            self.advance();
            let right = self.parse_parallel()?;
            left = ChainNode::Serial {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// parallel = pipeline (parallel_op pipeline)*
    fn parse_parallel(&mut self) -> Result<ChainNode, ParseError> {
        let mut left = self.parse_pipeline()?;
        loop {
            let op = match self.peek() {
                Token::ParallelAll => ParallelOp::All,
                Token::ParallelRace => ParallelOp::Race,
                _ => break,
            };
            self.advance();
            let right = self.parse_pipeline()?;
            left = ChainNode::Parallel {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// pipeline = atom (pipe_op atom)*
    fn parse_pipeline(&mut self) -> Result<ChainNode, ParseError> {
        let first = self.parse_atom_words()?;

        // Check for pipe operators
        let mut segments = vec![];
        let mut current_words = first;
        loop {
            let pipe_op = match self.peek() {
                Token::PipeStdout => PipeOp::Stdout,
                Token::PipeAll => PipeOp::StdoutStderr,
                Token::PipeStderr => PipeOp::StderrOnly,
                _ => {
                    // End of pipeline
                    if current_words.is_empty() {
                        break;
                    }
                    segments.push(PipeSegment {
                        command: current_words,
                        pipe_to_next: None,
                    });
                    break;
                }
            };
            segments.push(PipeSegment {
                command: current_words,
                pipe_to_next: Some(pipe_op),
            });
            self.advance(); // consume pipe op
            current_words = self.parse_atom_words()?;
        }

        if segments.is_empty() {
            return Err(ParseError {
                span: self.peek_span(),
                message: "expected command".into(),
                kind: ParseErrorKind::MissingArgument,
                suggestions: vec![],
            });
        }

        Ok(ChainNode::Leaf(Pipeline { segments }))
    }

    /// Parse words for one atom in a pipeline (or a grouped chain).
    fn parse_atom_words(&mut self) -> Result<Vec<String>, ParseError> {
        // Grouped chain: ( chain )
        if matches!(self.peek(), Token::GroupOpen) {
            // For grouped chains within pipes, this is illegal (chain can't pipe to process).
            // But for grouped chains at chain level, it's valid.
            // We handle this at the chain level instead.
        }

        let mut words = vec![];
        loop {
            match self.peek() {
                Token::Word(w) => {
                    let w = w.clone();
                    self.advance();
                    words.push(w);
                }
                Token::IdRef(k, n) => {
                    let s = format!("{k}{n}");
                    self.advance();
                    words.push(s);
                }
                Token::Command(c) => {
                    // In chain context, a `:cmd` is treated as a word
                    let s = format!(":{c}");
                    self.advance();
                    words.push(s);
                }
                _ => break,
            }
        }
        Ok(words)
    }
}

pub(super) fn parse_duration_str(s: &str) -> Option<std::time::Duration> {
    if s.ends_with("ms") {
        let n: u64 = s.strip_suffix("ms")?.parse().ok()?;
        return Some(std::time::Duration::from_millis(n));
    }
    for (suffix, multiplier) in [("s", 1u64), ("m", 60), ("h", 3600)] {
        if s.ends_with(suffix) {
            let n: u64 = s.strip_suffix(suffix)?.parse().ok()?;
            return Some(std::time::Duration::from_secs(n * multiplier));
        }
    }
    None
}

fn suggest_command(name: &str) -> Vec<String> {
    let commands = [
        "run", "kill", "retry", "out", "tail", "err", "fg", "wait", "send", "cancel", "jobs",
        "crons", "scopes", "cron", "env", "cd", "scope", "help", "config", "log", "pause", "wrap",
        "resume", "clear", "quit", "exit",
    ];
    commands
        .iter()
        .filter(|c| c.starts_with(&name[..1.min(name.len())]) || edit_distance(name, c) <= 2)
        .map(|c| format!(":{c}"))
        .collect()
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in dp.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in dp[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[a.len()][b.len()]
}

#[cfg(test)]
mod tests {
    use super::super::token::IdKind;
    use super::*;

    #[test]
    fn parse_simple_run() {
        let ast = Parser::parse(":run cargo test").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "run");
                match argument {
                    Argument::Chain(ChainNode::Leaf(p)) => {
                        assert_eq!(p.segments.len(), 1);
                        assert_eq!(p.segments[0].command, vec!["cargo", "test"]);
                    }
                    _ => panic!("expected Chain"),
                }
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_bare_input() {
        let ast = Parser::parse("cargo test --release").unwrap();
        match ast {
            Ast::BareInput { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments[0].command, vec!["cargo", "test", "--release"]);
                }
                _ => panic!("expected Chain"),
            },
            _ => panic!("expected BareInput"),
        }
    }

    #[test]
    fn parse_bare_input_with_numeric_arg() {
        let ast = Parser::parse("sleep 4").unwrap();
        match ast {
            Ast::BareInput { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments[0].command, vec!["sleep", "4"]);
                }
                _ => panic!("expected Chain"),
            },
            _ => panic!("expected BareInput"),
        }
    }

    #[test]
    fn parse_kill_id() {
        let ast = Parser::parse(":kill J1").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "kill");
                assert_eq!(argument, Argument::IdRef(IdKind::Job, 1));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_chain() {
        let ast = Parser::parse(":run a -> b || c").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Serial { op, left, right }) => {
                    assert_eq!(op, SerialOp::Then);
                    assert!(matches!(*left, ChainNode::Leaf(_)));
                    assert!(matches!(*right, ChainNode::Parallel { .. }));
                }
                _ => panic!("expected Chain with Serial"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_pipeline() {
        let ast = Parser::parse(":run a |> b |&> c").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments.len(), 3);
                    assert_eq!(p.segments[0].pipe_to_next, Some(PipeOp::Stdout));
                    assert_eq!(p.segments[1].pipe_to_next, Some(PipeOp::StdoutStderr));
                    assert!(p.segments[2].pipe_to_next.is_none());
                }
                _ => panic!("expected pipeline"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_mode_params() {
        let ast = Parser::parse(":run(retry=3) cargo test").unwrap();
        match ast {
            Ast::Command {
                mode_params,
                argument,
                ..
            } => {
                assert_eq!(mode_params.len(), 1);
                assert_eq!(mode_params[0].0, "retry");
                assert_eq!(mode_params[0].1, Value::Int(3));
                assert!(matches!(argument, Argument::Chain(_)));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_send_text() {
        let ast = Parser::parse(":send J1 continue with the fix").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "send");
                assert_eq!(argument, Argument::Text("J1 continue with the fix".into()));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_send_raw_preserves_operators() {
        // `:send` should preserve `->` as literal text, not as chain operator.
        let ast = Parser::parse(":send J1 replace a->b with c->d").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "send");
                assert_eq!(argument, Argument::Text("J1 replace a->b with c->d".into()));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_cron() {
        let ast = Parser::parse(":cron every 5m cargo test").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "cron");
                assert!(matches!(argument, Argument::Chain(ChainNode::Leaf(_))));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_empty_command() {
        let ast = Parser::parse(":jobs").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "jobs");
                assert_eq!(argument, Argument::Empty);
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn unknown_command_error() {
        let err = Parser::parse(":foo").unwrap_err();
        assert_eq!(err.kind, ParseErrorKind::UnknownCommand);
        assert!(!err.suggestions.is_empty());
    }

    #[test]
    fn missing_run_argument() {
        let err = Parser::parse(":run").unwrap_err();
        assert_eq!(err.kind, ParseErrorKind::MissingArgument);
    }

    #[test]
    fn complex_chain_with_pipes() {
        // cargo build |> grep error -> cargo test || cargo clippy
        let ast =
            Parser::parse(":run cargo build |> grep error -> cargo test || cargo clippy").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Serial { left, right, .. }) => {
                    // left = pipeline (cargo build |> grep error)
                    if let ChainNode::Leaf(p) = *left {
                        assert_eq!(p.segments.len(), 2);
                        assert_eq!(p.segments[0].command, vec!["cargo", "build"]);
                        assert_eq!(p.segments[0].pipe_to_next, Some(PipeOp::Stdout));
                        assert_eq!(p.segments[1].command, vec!["grep", "error"]);
                    } else {
                        panic!("expected Leaf pipeline");
                    }
                    // right = parallel (cargo test || cargo clippy)
                    assert!(matches!(*right, ChainNode::Parallel { .. }));
                }
                _ => panic!("expected Serial chain"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn single_quoted_args_with_colon() {
        // Regression: single quotes should be treated as literal string
        // delimiters, not as regular characters.  The `:` inside `'[:upper:]'`
        // must NOT be parsed as a command prefix.
        let ast = Parser::parse(":run tr '[:upper:]' '[:lower:]'").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments.len(), 1);
                    assert_eq!(p.segments[0].command, vec!["tr", "[:upper:]", "[:lower:]"]);
                }
                _ => panic!("expected single-segment pipeline"),
            },
            _ => panic!("expected Command"),
        }
    }
}
