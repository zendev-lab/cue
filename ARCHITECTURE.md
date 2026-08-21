# Cue architecture

Cue is a persistent local execution runtime. The daemon accepts typed
`ExecutionSpec` values and owns execution, session/scope, resource, process,
PTY, schedule, output, idempotency, and persistence state. Language and UI are
clients.

The shortest contract is:

```text
ExecutionSpec + session scope + launch context
    -> ExecutionId + stable StepIds + state/events/output
```

Start with:

- [Design index](docs/design/README.md)
- [Core types](docs/design/core-types.md)
- [Daemon architecture](docs/design/daemon-architecture.md)
- [IPC v3](docs/design/ipc-protocol.md)
- [Project direction](SPARK.md)

Agent/workflow policy, approval, secrets, fleet coordination, and general DAG
runtime semantics belong above Cue.
