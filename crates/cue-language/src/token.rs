use std::fmt;

/// Fine-grained token types produced by the Tokenizer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Command prefix
    /// `:` prefix for builtin commands.
    Colon,
    /// Command name immediately after `:`, e.g. `run`, `kill`, `jobs`.
    Command(String),

    // Mode params (context-sensitive: immediately after Command)
    /// `(` in mode-params context.
    ModeParenOpen,
    /// `)` in mode-params context.
    ModeParenClose,
    /// `=` in mode-params.
    ParamEq,
    /// Parameter value in mode-params.
    ParamValue(Value),
    /// `,` separator in mode-params.
    Comma,

    // Chain operators (job-level)
    /// `->` serial-then.
    SerialThen,
    /// `~>` serial-always.
    SerialAlways,
    /// `|||` parallel-all.
    ParallelAll,
    /// `|?|` parallel-race.
    ParallelRace,
    /// `&&` job-internal AND.
    JobAnd,
    /// `||` job-internal OR.
    JobOr,

    // Pipe operators (process-level, within a job)
    /// `|>` stdout pipe.
    PipeStdout,
    /// `|&>` stdout+stderr pipe.
    PipeAll,
    /// `|!>` stderr-only pipe.
    PipeStderr,

    // Grouping (chain-level)
    /// `(` for chain grouping.
    GroupOpen,
    /// `)` for chain grouping.
    GroupClose,

    // Content
    /// A word (command argument, filename, flag, etc.)
    Word(String),
    /// An entity ID reference like J1 or C3.
    IdRef(IdKind, u32),
    /// Unquoted bash syntax cue-shell does not interpret.
    ///
    /// Kept as a token rather than a tokenizer error so raw-text builtins such
    /// as `:send` can still carry these bytes as literal payload; the parser
    /// rejects it only where a command is expected.
    ShellSyntax(ShellSyntax),

    // Whitespace (preserved for highlighting, skipped during parsing)
    Whitespace(String),
    Newline,

    // Sentinel
    Eof,
}

/// Entity ID prefix kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdKind {
    Job,
    Cron,
}

/// An unquoted bash construct cue-shell recognizes only to reject.
///
/// cue-shell never hands a command line to a shell, so these bytes cannot mean
/// what bash would make them mean. Recognizing them explicitly is what turns a
/// silently wrong argv into a diagnosable error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShellSyntax {
    pub kind: ShellSyntaxKind,
    /// The exact operator text matched in the input.
    pub text: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellSyntaxKind {
    /// `>`, `>>`, `<`, `2>`, `&>`, `>&`, `<<`, ... — file/fd redirection.
    Redirect,
    /// `;` — command separator.
    Semicolon,
    /// `$(` or a backtick — command substitution.
    CommandSubstitution,
}

impl ShellSyntax {
    /// Human-readable name of the construct, for error messages.
    pub fn label(self) -> &'static str {
        match self.kind {
            ShellSyntaxKind::Redirect => "redirection",
            ShellSyntaxKind::Semicolon => "`;` command separator",
            ShellSyntaxKind::CommandSubstitution => "command substitution",
        }
    }

    /// The cue-shell equivalent, or why there is none.
    pub fn hint(self) -> &'static str {
        match self.kind {
            ShellSyntaxKind::Redirect => {
                "cue-shell runs commands directly instead of through a shell, so redirection would not be applied; pipe with `|>` / `|&>` / `|!>`, or let the command write the file itself"
            }
            ShellSyntaxKind::Semicolon => {
                "use `->` to continue on success, `~>` to continue regardless, or `&&` / `||` to stay inside one job"
            }
            ShellSyntaxKind::CommandSubstitution => {
                "command substitution needs a shell; run the inner command as its own job and pass its result explicitly"
            }
        }
    }

    /// Suggested rewrites offered alongside the parse error.
    pub fn suggestions(self) -> Vec<String> {
        match self.kind {
            ShellSyntaxKind::Redirect => vec![
                "producer |> consumer".into(),
                "producer |&> consumer".into(),
            ],
            ShellSyntaxKind::Semicolon => {
                vec!["first -> second".into(), "first ~> second".into()]
            }
            ShellSyntaxKind::CommandSubstitution => Vec::new(),
        }
    }
}

impl fmt::Display for ShellSyntax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.text)
    }
}

/// Typed value in mode-params.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Bool(bool),
}

/// A token with its byte-offset span in the original input.
#[derive(Debug, Clone, PartialEq)]
pub struct Spanned {
    pub token: Token,
    pub span: Span,
}

/// Byte-offset range in the original input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

impl fmt::Display for IdKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Job => "J",
            Self::Cron => "C",
        })
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Colon => f.write_str(":"),
            Self::Command(s) => write!(f, "{s}"),
            Self::ModeParenOpen | Self::GroupOpen => f.write_str("("),
            Self::ModeParenClose | Self::GroupClose => f.write_str(")"),
            Self::ParamEq => f.write_str("="),
            Self::ParamValue(v) => write!(f, "{v:?}"),
            Self::Comma => f.write_str(","),
            Self::SerialThen => f.write_str("->"),
            Self::SerialAlways => f.write_str("~>"),
            Self::ParallelAll => f.write_str("|||"),
            Self::ParallelRace => f.write_str("|?|"),
            Self::JobAnd => f.write_str("&&"),
            Self::JobOr => f.write_str("||"),
            Self::PipeStdout => f.write_str("|>"),
            Self::PipeAll => f.write_str("|&>"),
            Self::PipeStderr => f.write_str("|!>"),
            Self::Word(s) => write!(f, "{s}"),
            Self::IdRef(k, n) => write!(f, "{k}{n}"),
            Self::ShellSyntax(s) => write!(f, "{s}"),
            Self::Whitespace(s) => write!(f, "{s}"),
            Self::Newline => f.write_str("\\n"),
            Self::Eof => f.write_str("<EOF>"),
        }
    }
}

impl Token {
    pub fn operator_text(&self) -> &'static str {
        match self {
            Self::SerialThen => "->",
            Self::SerialAlways => "~>",
            Self::ParallelAll => "|||",
            Self::ParallelRace => "|?|",
            Self::JobAnd => "&&",
            Self::JobOr => "||",
            Self::PipeStdout => "|>",
            Self::PipeAll => "|&>",
            Self::PipeStderr => "|!>",
            _ => "",
        }
    }
}
