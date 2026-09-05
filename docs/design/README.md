# Cue design index

These documents describe the shipped IPC v4 architecture. [FP-0001](../../fps/FP-0001-structured-execution-kernel.md) owns the normative
contract; these documents describe its implementation.

- [Execution kernel](kernel.md): product boundary, closed plan ADT, Scope,
  process environment, PTY topology, and Composition laws.
- [Reducer](reducer.md): Step state, Scope threading, parallel semantics,
  cancellation, and restart interruption.
- [Protocol and store](protocol.md): command/query separation, fact replay,
  operation idempotency, explicit sensitivity, and SQLite schema.
- [Runtime](runtime.md): typed Assembly, captured/PTY pipeline realization,
  output offsets, control, and recovery.
- [Language](language.md): explicit Scope input, three builtins,
  process-local assignments, per-Run PTY, and external-owner diagnostics.
- [Daemon](daemon.md): bootstrap, strict serving, atomic commands, facts,
  lifecycle, and PTY attachments.
- [Client](client.md): frontend Scope snapshots, typed dispatch, correlation,
  and multiplexed response/event routing.
- [Frontends](frontends.md): CLI, PTY passthrough, TUI, and extension boundary.

Research notes under `docs/research/` are historical inputs, not current API or
compatibility promises.
