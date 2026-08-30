# Cue design index

Cue vNext is being built as a hard cut from the current IPC v3 runtime. The
vNext contract is frozen in the first document below; the remaining documents
describe the implementation being replaced until their corresponding migration
lands. Source types and strict wire validators remain authoritative when prose
and code disagree.

- [vNext foundation](vnext-foundation.md): target boundary, closed execution ADT,
  explicit Scope, and bootstrap Composition.
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
