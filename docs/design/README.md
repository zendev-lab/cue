# Cue design index

Cue vNext is being built as a hard cut from the current IPC v3 runtime. The
vNext contract is frozen in the first document below; the remaining documents
describe the implementation being replaced until their corresponding migration
lands. Source types and strict wire validators remain authoritative when prose
and code disagree.

- [vNext foundation](vnext-foundation.md): target boundary, closed execution ADT,
  explicit Scope, and bootstrap Composition.
- [vNext reducer](vnext-reducer.md): durable Step state, Scope propagation,
  condition/parallel semantics, cancellation, and restart interruption.
- [vNext protocol and store](vnext-protocol.md): v4 command/query separation,
  fact replay, operation idempotency, and fresh SQLite provider schema.
- [vNext runtime](vnext-runtime.md): typed Assembly binding, captured/PTY
  pipeline realization, output offsets, control, and restart recovery.
- [vNext language](vnext-language.md): explicit Scope input, three builtins,
  process-local assignments, per-Run PTY, and external-owner diagnostics.
- [vNext daemon](vnext-daemon.md): runtime assembly, v4 serving, atomic
  commands, event replay, volatile secrets, and PTY attachment ownership.
- [vNext client](vnext-client.md): explicit frontend Scope snapshots, operation
  identities, typed dispatch, and concurrent response/event routing.
- [vNext frontends](vnext-frontends.md): CLI command surface, PTY passthrough,
  execution TUI, and the removal of kernel-owned policy screens.
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
