# vNext daemon service

The vNext daemon is the composition root and protocol host for the closed Core
semantics. It does not parse Cue source and does not reuse the IPC v3 actor,
session, schedule, resource, retry, or persistence owners.

## Bootstrap

At startup the daemon resolves the canonical runtime ports through
`Composition`, then binds a typed `RuntimeAssembly`. The hot path receives the
resolved scope store, execution store, output store, and process spawner; it
does not query a service locator while executing work.

Recovery reads every non-terminal projection. Any step persisted as `Running`
is first failed as an infrastructure interruption, committed as facts, and
then passed back through normal reducer advancement.

## Command boundary

Every connection must begin with `Hello` and binds one immutable `ClientId`.
Queries are read-only. Commands carry an `OperationId` and are claimed by the
store together with their effect:

- durable `PutScope` and `SubmitExecution` commit the operation and value in
  one SQLite transaction;
- replay returns the original response without repeating the effect;
- reuse with a different typed command is a conflict;
- expired response bodies leave permanent at-most-once tombstones;
- secret-bearing scopes, executions, facts, and operation responses remain in
  memory and disappear on restart.

The wire format is strict IPC v4 framing: a four-byte big-endian payload length
followed by one validated message. Unknown fields, wrong message roles, and
oversized frames are rejected.

## Execution and observation

Submission persists `ExecutionCreated` before scheduling. The daemon asks the
pure reducer for ready leaves, marks them running, commits the projection and
facts, and only then realizes builtins or processes. Builtins are exactly
`Cd`, `Env`, and `Umask`; runs use the typed local pipeline runner. Completion
returns to the reducer, including Sequence scope threading and Parallel
fork/no-merge behavior.

`WatchExecution` replays facts after the supplied cursor before forwarding
live facts. Output is addressed by stable `StepId`, stream, and absolute byte
offset. Captured runs expose stdout/stderr; PTY runs expose one terminal stream.

PTY attachments are connection-owned observer leases. At most one attachment
per Step is the controller. Only that controller can write input or resize the
terminal; every attachment may receive the same terminal output stream.

## Owner boundary

The daemon accepts only fully resolved `ExecutionSpec` values. Surface parsing,
assignment expansion, named sessions, schedules, retry policy, admission,
approval, and resource selection belong to clients or external producers. The
hard cut intentionally provides no v3 compatibility bridge.
