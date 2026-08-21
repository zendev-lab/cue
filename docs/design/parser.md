# Cue language frontend

`cue-language` owns tokenization, parsing, resolution, completion,
highlighting, and compilation. CLI and TUI use it locally; `cued` does not
depend on it.

## Pipeline and composition

```text
pipeline       = segment ("|>" segment)*
segment        = assignment* command argument*
assignment     = NAME "=" value
execution      = pipeline
               | execution ("&&" | "->") execution
               | execution "||" execution
               | execution "~>" execution
               | execution "|||" execution
               | execution "|?|" execution
```

Operators must be separated from adjacent words. Quoted operators remain argv
data. Unquoted shell redirection, command substitution, backgrounding, and
other shell-only syntax are rejected with a Cue equivalent where available.
Cue never passes the input line to a shell.

Leading assignment words are extracted into the current `PipeSegment.env`:

```cue
A=1 B=two command --flag C=argument
```

Here `A` and `B` are process-local environment overrides; `C=argument`
is argv because it appears after the executable. Each pipeline segment has an
independent environment map.

## Compilation

The compiler maps syntax directly to typed nodes:

- `|>` -> segments of one `Pipeline`;
- `&&` and `->` -> `OnSuccess`;
- `||` -> `OnFailure`;
- `~>` -> `Always`;
- `|||` -> `ParallelAll`;
- `|?|` -> `AnySuccess`;
- typed scope commands -> `ContextDelta`.

Interactive builtins such as session, schedule, output, and PTY operations
resolve to typed client intents. Help, clear, and quit remain frontend-only.
Local AST names are compiler implementation details and never become daemon
state entities.

`.cue` source uses newline as a top-level item separator only after the
current expression is complete. Shebang and comment-only lines are ignored.
Completion and highlighting consume the same token/command metadata as the
compiler, preventing a daemon-owned duplicate command table.
