# Cue design index

Cue vNext is being built as a hard cut from the current IPC v3 runtime.
[FP-0001](../../fps/FP-0001-structured-execution-kernel.md) defines the public
execution contract under [FP-0000](../../fps/FP-0000-governance.md).
The vNext documents describe its implementation; the IPC v3 documents remain
until their corresponding migration lands.

- [vNext foundation](vnext-foundation.md): target boundary, closed execution ADT,
  explicit Scope, and bootstrap Composition.
- [vNext reducer](vnext-reducer.md): durable Step state, Scope propagation,
  condition/parallel semantics, cancellation, and restart interruption.
- [vNext protocol and store](vnext-protocol.md): v4 command/query separation,
  fact replay, operation idempotency, and fresh SQLite provider schema.
- [Core types](core-types.md): `ExecutionSpec`, plan composition, IDs, states.
- [Daemon architecture](daemon-architecture.md): state ownership, actors, spawn path, persistence.
- [IPC protocol](ipc-protocol.md): framing, handshake, requests, responses, events.
- [Cue language](parser.md): frontend grammar, resolution, and compilation.
- [Cue files](cue-script.md): `.cue` source compilation and execution.
- [Commands and modes](commands-and-modes.md): interactive frontend behavior.
- [TUI](tui.md): interactive observation and PTY control.
- [Transport modes](transport-modes.md): local Unix and explicit SSH transport.
- [Sandbox threat model](sandbox-threat-model.md): workspace view and adapter trust boundaries.
- [TUI debug control](tui-debug-control.md): debug-only frontend automation.

Historical names and v2 J/CH/R examples belong only in migration fixtures.
They are not compatibility promises.
