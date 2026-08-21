# `cued` architecture

`cued` is the single local owner of execution, session/scope, process, PTY,
resource, schedule, event, idempotency, and persistence state. It accepts typed
IPC v3 requests; it does not contain the Cue language parser or host-specific
policy.

## Runtime owners

| Owner | Responsibility | Persistent state |
| --- | --- | --- |
| Gateway | strict framing, handshake, routing, subscriptions, operation ledger | idempotency facts |
| SessionCoordinator | named/anonymous attachment, scope cursor, archive safety | sessions |
| ExecutionCoordinator | the only mutable `Execution` reducer and projections | executions, steps |
| TriggerService | schedule definitions and timers; submit fresh executions | schedules |
| ProcessManager | run one ready pipeline step, process groups, PTY, I/O | output files |
| ScopeStore | immutable scopes and volatile sensitive-scope cache | scopes |
| EventBus | bounded session-aware fan-out | none |

There is no scheduler actor that also owns jobs, chains, scripts, crons, help,
configuration formatting, and process orchestration. TriggerService never saves
execution state; ProcessManager never decides conditional or parallel plan
semantics.

## Execution flow

```text
SubmitExecution
  -> validate and allocate E<n>
  -> reducer derives ready nodes
  -> resource admission
  -> one ProcessManager pipeline step
  -> StepStateChanged / OutputChunk
  -> process outcome
  -> reducer advances or cancels branches
  -> ExecutionFinished
```

Each actual pipeline segment passes exactly once through `prepare_spawn`:

1. resolved scope/environment and resource reservation;
2. argv expansion;
3. workspace view;
4. configured wrapper;
5. optional SpawnAdapter;
6. command construction and spawn.

PTY and pipe paths differ only in file-descriptor/process-group wiring after
preparation. This prevents wrappers, workspace views, or policy adapters from
being applied inconsistently.

## SpawnAdapter

An execution may carry a non-persistent adapter handle: a private local Unix
socket plus opaque token. Before spawning each segment, `PrepareSpawn` may
replace final argv or reject it. After exit, `SettleSpawn` receives exit code,
signal/spawn error, and a bounded diagnostic tail.

Security properties:

- endpoint must be inside Cue's adapter runtime directory;
- socket and parent permissions are private and peer UID must match;
- token is never copied into env, SQLite, output, or events;
- prepare failure means no original command is launched;
- settle transport failure produces execution infrastructure failure;
- scheduled templates cannot persist ephemeral handles.

The protocol contains no DSH names or approval semantics. A DSH host can supply
a broker, while other hosts can implement the same generic contract.

## Persistence and restart

Schema v21 stores only scopes, sessions, executions, execution steps, schedules,
and operation idempotency facts. Output remains in per-step files. Ephemeral
adapter secrets and sensitive scope environments are never written.

Startup restores projections before readiness. Running steps interrupted by a
daemon stop become explicit infrastructure outcomes and the reducer advances
only reachable failure/always branches. Completed operation facts are loaded
before accepting requests, preventing a reconnect or restart from executing the
same side effect twice.

Restart is drain-first and instance-fenced. Upgrade migration obtains the same
instance lock before archiving a v18 legacy database. The live runtime never
dual-reads legacy J/CH/R or cron history.
