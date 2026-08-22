# Command Parser Design

## 1. Architecture

**Three-layer pipeline**, owned by the I/O-free `cue-language` crate:

```
Raw input (String)
  → Tokenizer  → Vec<Token>
  → Parser     → Ast / Script (unresolved)
  → Resolver   → ResolvedCommand (validated, ready for execution)
```

Frontends use this pipeline before submitting typed execution contracts. During
the internal IPC v2-to-v3 stack migration, cued temporarily calls the same
library for legacy `Eval`; this bridge is removed with the v3 cut. File scripts
loaded through `cue run <file.cue>` use the same compiler with source metadata.

File-script bodies are parsed as a **top-level script**: newline separates items
only at top level, and only when the current chain is already syntactically
complete. Interactive JOB multiline input is not a script-mode entry point; see
[cue-script.md](cue-script.md).

## 2. Implementation

**Hand-written recursive descent** — no parser combinator dependencies.
- Full control over error messages, recovery, and completion suggestions
- Tokenizer is a state machine with context-sensitive `()` handling
- Parser produces an unresolved AST; Resolver validates IDs, scopes, injects mode defaults

## 3. Token Types

```rust
#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Command prefix
    Colon,                  // :
    Command(String),        // run, kill, jobs, cron, ...

    // Mode params (context: immediately after Command)
    ModeParenOpen,          // ( — mode params context
    ModeParenClose,         // ) — mode params context
    ParamEq,               // =
    ParamValue(Value),      // true, false
    Comma,                  // ,

    // Operators (chain layer)
    SerialThen,             // ->
    SerialAlways,           // ~>
    ParallelAll,            // |||
    ParallelRace,           // |?|

    // Operators (pipe layer, within a job)
    PipeStdout,             // |>
    PipeAll,                // |&>
    PipeStderr,             // |!>

    // Grouping (chain layer)
    GroupOpen,              // (
    GroupClose,             // )

    // Content
    Word(String),           // command arguments, filenames, flags
    IdRef(IdKind, u32),     // J1, C3
    ShellSyntax(ShellSyntax), // unquoted bash syntax, recognized to reject

    // Whitespace (preserved for highlighting, stripped for parsing)
    Whitespace(String),
    Newline,

    // Special
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
enum IdKind { Job, Cron }

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Str(String),
    Bool(bool),
}
```

### Word assembly and quoting

A `Word` is the concatenation of unquoted runs and quoted segments, so shell
adjacency holds: `--msg="a b"` is one argument, `a'b'c` is `abc`. Quotes are
lexical boundaries, not argument boundaries, and are removed from the token.
Double quotes interpret `\"`, `\\`, `\n`, `\t` and preserve any other backslash
pair; single quotes are fully literal. Text inside quotes is exempt from
operator, delimiter, and shell-syntax scanning, which is how `echo 'a; b'` and
`echo "c > d"` pass metacharacters through as data.

### Unquoted bash syntax

cue-shell runs commands directly and never hands a command line to a shell, so
`>`, `>>`, `<`, `2>`, `&>`, `>&`, `<<`, `;`, `$(`, and backticks cannot mean what
bash would make them mean. They tokenize as `ShellSyntax` rather than being
absorbed into words, and the parser rejects them wherever a command is expected
with `ParseErrorKind::UnsupportedShellSyntax`, naming the construct and the
cue-shell equivalent. Keeping them as a token instead of a tokenizer error lets
raw-text builtins such as `:send` still carry those bytes as literal payload.

### `()` Disambiguation

Tokenizer uses positional context:
- Previous non-whitespace token is `Command(...)` → `ModeParenOpen` / `ModeParenClose`
- Otherwise → `GroupOpen` / `GroupClose`

## 4. AST (Unresolved)

```rust
enum Ast {
    Script {
        items: Vec<ScriptItemAst>,
    },
    Command {
        name: String,
        mode_params: Vec<(String, Value)>,
        argument: Argument,
    },
    BareInput {
        argument: Argument,
    },
}

struct ScriptItemAst {
    source: String,
    statement: Box<Ast>,
}

/// Argument types — which variant depends on command
enum Argument {
    Chain(ChainNode),           // for :run, :cron's body, bare input in JOB/CRON mode
    IdRef(IdKind, u32),         // for :kill, :out, :fg, :retry, etc.
    Text(String),               // for :send and similar text-taking commands
    CronExpr {                  // for :cron
        schedule: CronScheduleAst,
        body: ChainNode,
    },
    Empty,                      // for :jobs, :crons, :scopes, :help
}

/// Chain AST — tree structure
enum ChainNode {
    Leaf(Pipeline),
    Serial { op: SerialOp, left: Box<ChainNode>, right: Box<ChainNode> },
    Parallel { op: ParallelOp, left: Box<ChainNode>, right: Box<ChainNode> },
}

enum SerialOp { Then, Always }   // ->  ~>
enum ParallelOp { All, Race }    // |||  |?|

/// Pipeline = one Job's process chain
struct Pipeline {
    segments: Vec<PipeSegment>,
}

struct PipeSegment {
    env: BTreeMap<String, String>, // process-local prefix assignments
    command: Vec<String>,        // ["cargo", "test", "--release"]
    pipe_to_next: Option<PipeOp>,
}

enum PipeOp { Stdout, All, Stderr }  // |>  |&>  |!>

/// Unresolved cron schedule variants. Resolver validates into core CronSchedule.
enum CronScheduleAst {
    Every(Duration),                         // every 5m
    At(TimeSpec),                            // at 03:00
    In(Duration),                            // in 30s (one-shot)
    Cron(String),                            // validated into core CrontabSchedule
}
```

## 5. Grammar (EBNF)

```ebnf
input       = statement (newline statement)*
statement   = command | bare_input
command     = ":" cmd_name mode_params? argument
bare_input  = argument

cmd_name    = IDENT
mode_params = "(" param_list ")"
param_list  = param ("," param)*
param       = IDENT "=" value
value       = NUMBER | DURATION | STRING | BOOL

argument    = chain | id_ref | cron_expr | text | empty

chain       = parallel (serial_op parallel)*
parallel    = job_expr (parallel_op job_expr)*
job_expr    = pipeline (("&&" | "||") pipeline)*
pipeline    = atom (pipe_op atom)*
atom        = "(" chain ")"
            | assignment* word+

serial_op   = "->" | "~>"
parallel_op = "|||" | "|?|"
pipe_op     = "|>" | "|&>" | "|!>"

id_ref      = [JCS] DIGITS
word        = (raw_run | quoted_segment)+
assignment  = env_name "=" word?
env_name    = (ALPHA | "_") (ALPHA | DIGIT | "_")*
raw_run     = <non-operator, non-delimiter, non-shell-syntax bytes>
quoted_segment = "'" <literal bytes> "'"
            | '"' <bytes with \" \\ \n \t escapes> '"'

cron_expr   = schedule "do" chain
schedule    = "every" DURATION
            | "at" TIME
            | "in" DURATION
            | "cron" QUOTED_STRING
```

Notes:

- in file-script source mode, newline is a script-item separator only at the top level
- newline inside an unfinished chain behaves like whitespace, so operators can be
  wrapped across lines naturally
- resolver can therefore return either one normal command or one script command
  containing multiple resolved top-level items
- adjacent raw runs and quoted segments form one `word`, so a quote never splits
  an argument
- leading `VAR=value` words are typed process-local environment overrides for
  that pipeline segment; later segments and the session scope are unchanged
- assignment values use the same tilde and environment expansion as command
  words, and an assignment-only segment is rejected because it has no process
- unquoted shell syntax is not part of `word`; it becomes its own token and is
  rejected in command position

## 6. Resolver

The Resolver transforms `Ast` into a validated frontend intent:

1. **Mode injection**: `BareInput` → wraps with default command per current mode
   - JOB ⚡ → `:run`
   - CRON ⏰ → `:cron`
   - `.cue` file scripts resolve bare top-level items in JOB mode, so `echo hi`
     in a script is equivalent to `:run echo hi` while preserving source text

2. **Argument type validation**: ensures command gets correct argument type
    - `:run` expects Chain, `:kill` expects IdRef, `:send` expects Text, etc.

3. **ID syntax**: classifies typed references; live existence is checked by the
   daemon operation that owns the referenced entity

4. **Mode params parse/validate**: per-invocation params are validated against
   `cue-language::command_spec`; dynamic namespaces such as `need.<resource>` are
   still gated by that metadata.

5. **Scope resolution**: jobs and crons start from the caller's session cursor
   or an explicit scope override, then `cwd=...` can derive a start scope without
   mutating that cursor. Launch options such as `pty`, `need.*`, and `sandbox.*`
   are validated from command metadata but do not enter the scope hash. The
   concrete scope schema lives in `crates/cue-core/src/scope.rs`.

## 7. Completion Service

Completion runs locally through `cue-language`; it does not require daemon RPC:

1. Tokenize up to cursor position
2. Determine context:
   - After `:` → command name completion (run, kill, jobs, ...)
   - After `:cmd(` → command-specific mode param key completion
   - After `=` in mode params → value completion (based on param type)
   - After IdRef prefix `J` → active job ID completion
   - After word → filesystem path / command completion
   - After operator → next segment (no completions, just indicate expected input)
3. Return candidates to the invoking frontend

```rust
struct CompletionItem {
    label: String,           // display text
    insert_text: String,     // text to insert
    kind: CompletionKind,    // Command, Param, Id, Path, Operator
    detail: Option<String>,  // description
}
```

## 8. Syntax Highlighting Service

Highlighting runs locally through the same tokenizer and returns spans:

```rust
struct HighlightSpan {
    range: Range<usize>,     // byte range in input
    kind: HighlightKind,
}

enum HighlightKind {
    CommandPrefix,   // :
    CommandName,     // run, kill, ...
    ModeParam,       // pty=false
    Operator,        // ->, |||, &&, |>, ...
    IdRef,           // J1, A2
    Word,            // arguments
    String,          // quoted strings
    Number,          // numeric values
    Error,           // invalid tokens
}
```

## 9. Error Handling

Parser produces structured errors for TUI display:

```rust
struct ParseError {
    span: Range<usize>,      // byte range of error
    message: String,          // human-readable message
    kind: ParseErrorKind,
    suggestions: Vec<String>, // "did you mean :run?"
}

enum ParseErrorKind {
    UnknownCommand,
    InvalidModeParam,
    UnexpectedToken,
    MissingArgument,
    InvalidIdRef,
    UnmatchedParen,
    InvalidOperator,
    InvalidCronSchedule,
    UnsupportedShellSyntax,
}
```

TUI highlights the error span in red and shows the message inline.

## 10. Command Classification Table

Which argument type each command expects:

| Command | Argument | Mode Params |
|---|---|---|
| `:run` | Chain | ✓ (see `cue-language::command_spec`) |
| `:cron` | Chain（resolver 再拆 schedule/body） | ✓ (see `cue-language::command_spec`) |
| `:kill` | Job/Cron IdRef (`J<n>` or `C<n>`) | ✗ |
| `:retry` | Job IdRef (`J<n>`) | ✗ |
| `:out` | Job IdRef (`J<n>`) | ✗ |
| `:tail` | Job IdRef (`J<n>`) + optional bytes | ✗ |
| `:err` | Job IdRef (`J<n>`) | ✗ |
| `:fg` | Job IdRef (`J<n>`) | ✗ |
| `:watch` | Job IdRef (`J<n>`) | ✗ |
| `:wait` | Job IdRef (`J<n>`) | ✗ |
| `:send` | Job target + raw text (`J<n> <input>`) | ✗ |
| `:cancel` | Job IdRef (`J<n>`) | ✗ |
| `:jobs` | Empty | ✗ |
| `:crons` | Empty | ✗ |
| `:scopes` | Empty | ✗ |
| `:providers` | Empty | ✗ |
| `:resources` | Empty | ✗ |
| `:env` | Text (subcommand) | ✗ |
| `:cd` | Text (path) | ✗ |
| `:scope` | Text (`list`; other subcommands not implemented) | ✗ |
| `:help` | Empty or Text | ✗ |
| `:pause` | Cron IdRef (`C<n>`) | ✗ |
| `:resume` | Cron IdRef (`C<n>`) | ✗ |
| `:config` | Text | ✗ |
| `:wrap` | Text (`on`, `off`, `status`) | ✗ |
| `:pty` | Text (`on`, `off`, `status`) | ✗ |
| `:log` | Job/Cron IdRef (`J<n>` or `C<n>`) or Empty | ✗ |
| `:clear` | Empty | ✗ |
| `:quit` | Empty | ✗ |
| `:exit` | Empty | ✗ |
