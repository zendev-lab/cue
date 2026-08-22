# `.cue` file contract

A `.cue` file is source for the client-side Cue language compiler. It is not a
daemon script object and does not create an `R<n>` identity.

`cue run path.cue` performs:

1. read and validate the local file;
2. tokenize, parse, and resolve with `cue-language`;
3. compile all top-level items into one typed `ExecutionPlan`;
4. attach file/line source metadata;
5. send one `SubmitExecution`;
6. wait for that execution and stream typed output events.

Top-level items execute in file order and stop on failure. Explicit composition
operators retain their normal semantics. A shebang and comment-only lines are
ignored.

```cue
# build.cue
RUST_LOG=debug cargo test
cargo fmt -> cargo clippy
```

Leading `NAME=value` assignments are process-local for the affected pipeline
segment. They neither invoke a shell nor update the session scope.

The daemon never parses this file, completes it, highlights it, or persists its
ephemeral SpawnAdapter lease. A transport disconnect does not destroy the
execution; reconnect with `GetExecution`, `WaitExecution`, and
`ReadExecutionOutput`.
