# Interactive commands and modes

The interactive UI is a client for the typed runtime. Its input mode selects
frontend defaults; it does not change the daemon protocol.

- execution mode compiles bare input to `SubmitExecution`;
- schedule mode compiles a schedule plus body to `CreateSchedule`;
- `:` commands expose explicit typed execution, step, scope, session, resource,
  and schedule operations;
- help, clear, target selection, and quit are local UI actions.

Examples:

```text
cargo test
RUST_LOG=debug cargo test
:executions
:wait E12
:cancel E12
:out E12
:fg E12/S1
:watch E12/S1
:schedule every 10m do cargo test
```

The authoritative unit shown in history is an execution (`E<n>`). A process
step (`E<n>/S<n>`) is shown only where output or PTY control needs a concrete
leaf. Schedule management uses typed `ScheduleId`; triggering always creates
a fresh execution.

Retry is a client operation: read the original spec and submit a new execution
with `retry_of`. Archive is reversible and refuses a named session with
connected clients, non-terminal executions, or owned schedules.

The language frontend may preserve familiar labels while migrating UI copy, but
it may not send `Eval`, infer state from display text, or recreate job/chain/
script stores.
