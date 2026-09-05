# Cue daemon service

The daemon is the composition root and protocol host for the closed Core
semantics. It does not parse Cue source and does not reuse the IPC v3 actor,
session, schedule, resource, retry, or persistence owners.

## Bootstrap

At startup the daemon resolves the canonical runtime ports through
`Composition`, then binds a typed `RuntimeAssembly`. The hot path receives the
resolved scope store, execution store, output store, and process spawner; it
does not query a service locator while executing work.

Recovery reclaims durable runtime work only after exclusive host ownership.
Unstarted Runs and replayable builtins may resume. A persisted physical Run
attempt with unknown ownership rejects startup before replay or new facts;
losing the supervisor does not establish process quiescence.
Recovery walks every stored page, including older active executions behind
newer terminal history.

## Command boundary

Every connection must begin with `Hello` and binds one immutable `ClientId`.
Queries are read-only. Commands carry an `OperationId` and are claimed by the
store together with their effect:

- durable `PutScope` and `SubmitExecution` commit the operation and value in
  one SQLite transaction;
- replay returns the original response without repeating the effect;
- reuse with a different typed command is a conflict;
- expired response bodies leave permanent at-most-once tombstones;
- explicitly Sensitive environment values are rejected as unsupported before
  persistence; names never infer sensitivity.

The wire format is strict IPC v4 framing: a four-byte big-endian payload length
followed by one validated message. Unknown fields, wrong message roles, and
oversized frames are rejected.
Partial frame state survives event delivery. Pending `WaitExecution` queries
run independently of the connection reader, so the same connection can still
query or cancel an execution. Mutation commands retain their receive order.

## Execution and observation

Submission persists `ExecutionCreated` before scheduling. The daemon asks the
pure reducer to atomically transition ready leaves to Running and return their
StepIds. It commits the candidate snapshot, facts, and durable follow-up work
before updating live state or publishing. Claimed workers read the latest
snapshot, and persist an attempt marker before physically starting a Run.
Generation-aware acknowledgements preserve newer cancellation work. Builtins are exactly
`Cd`, `Env`, and `Umask`; runs use the typed local pipeline runner. Completion
returns to the reducer, including Sequence scope threading and Parallel
fork/no-merge behavior.
On a store failure, the worker retains its generation and any known completion,
retries persistence, and releases the semantic state lock between attempts.
A physical Run is never respawned to retry its completion or acknowledgement.
Cancel replay checks the durable operation outcome before requiring a live task.

`WatchExecution` replays facts after the supplied cursor before forwarding
live facts. Output is addressed by stable `StepId`, stream, and absolute byte
offset. Captured runs expose stdout/stderr; PTY runs expose one terminal stream.
Replay reads every page through a cursor fixed with the stored snapshot, then
filters duplicate live facts at that boundary.

PTY attachments are connection-owned observer leases. At most one attachment
per Step is the controller. Only that controller can write input or resize the
terminal; every attachment may receive the same terminal output stream.
The lease belongs to the individual connection even when another connection
uses the same ClientId. EOF, transport errors, and connection task cancellation
release its attachments and controller role.

## Owner boundary

The daemon accepts only fully resolved `ExecutionSpec` values. Surface parsing,
assignment expansion, named sessions, schedules, retry policy, admission,
approval, and resource selection belong to clients or external producers. The
daemon provides no v3 compatibility bridge.

Lifecycle commands persist their outcome, then the connection writes and
flushes the response before signalling the host. The host closes admission
and drains owned Runs before releasing its exclusive lock or starting a
successor. No fixed delay substitutes for acknowledgement delivery.

If an acknowledgement is lost before flush, the same host retains the pending
lifecycle outcome across connections. Replaying its OperationId flushes the same
response before signalling once; a successor does not reapply its predecessor's
lifecycle commands.
