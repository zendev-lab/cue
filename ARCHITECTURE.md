# Cue architecture

Cue vNext is a persistent local structured-process kernel. The daemon accepts
typed `ExecutionSpec` values and owns execution, process, PTY, output,
idempotency, and persistence facts. Language, session cursors, schedules,
resource policy, and UI are clients or external producers.

The shortest contract is:

```text
ExecutionSpec { explicit ScopeHash, closed ExecutionPlan }
    -> ExecutionId + stable StepIds + state/events/output
```

`cue-core` fixes execution meaning. Bootstrap-time `cue-runtime` Composition
selects stores, spawner, workspace, transforms, guards, and observers, then
hands the daemon a resolved Assembly. Runtime execution never resolves services
dynamically.

The repository still contains the IPC v3 implementation while callers migrate.
New semantics live temporarily under `cue_core::vnext`; that namespace replaces
the old root modules at the v4 hard cut and is not exposed as a second daemon
protocol.

Start with:

- [Feature Proposal governance](fps/FP-0000-governance.md)
- [Design index](docs/design/README.md)
- [vNext foundation](docs/design/vnext-foundation.md)
- [Core types](docs/design/core-types.md)
- [Daemon architecture](docs/design/daemon-architecture.md)
- [Current IPC v3](docs/design/ipc-protocol.md)
- [Project direction](SPARK.md)

Agent/workflow policy, approval, secrets, fleet coordination, and general DAG
runtime semantics belong above Cue.
