# Cue daemon service

The daemon is the composition root and protocol host for the closed Core
semantics. It does not parse Cue source and does not reuse the IPC v3 actor,
session, schedule, resource, retry, or persistence owners.

## Host lifecycle

`cued start` (also bare `cued`) launches a detached background child with
`--fg`, no terminal descriptors, and logs appended to `<socket>.log`. The caller
waits up to fifteen seconds for a Hello from that specific new instance; another
listener cannot satisfy readiness. Child exit includes its startup log tail in
the error. A timeout is a failure to confirm readiness, not successful startup;
the diagnostic identifies the still-starting process and log. `--fg`/`-f` keeps
the serving process in the foreground for terminals and service managers.

The host holds exclusive locks for both its socket and canonical database path.
Independent sockets require independent databases. Custom paths are resolved
before spawning. Cue creates new private directories but preserves permissions
on existing user directories. Database, lock, socket, and log files are private.

`cued stop` waits for both listener unavailability and ownership-lock release,
because the listener closes before drain finishes. `cued restart` waits for the
instance named in RestartAccepted to answer Hello. Each completion wait is
bounded to fifteen seconds, covering the host's normal drain budget. IPC
Shutdown/Restart acknowledgements remain acceptance receipts; callers of raw
protocol commands, including `cue-client shutdown/restart`, must separately
observe completion. No kernel schema or lifecycle acknowledgement ordering changes.

## Bootstrap

At startup the daemon resolves the canonical runtime ports through
`Composition`, then binds a typed `RuntimeAssembly`. The hot path receives the
resolved scope store, execution store, output store, and process spawner; it
does not query a service locator while executing work.

Recovery reclaims durable runtime work only after exclusive host ownership.
Unstarted Runs and replayable builtins may resume. A persisted physical Run
attempt with unknown ownership rejects startup before replay or new facts;
losing the supervisor does not establish process quiescence.
`OwnershipLost` leaves the affected Step Running/Cancelling. Its durable
attempt marker also prevents the next host from starting. There is currently
no operator abandon/mark-lost command or supported in-place recovery procedure.
Do not clear attempt markers or turn an unknown attempt into a terminal outcome:
that would allow duplicate effects or dependent work without quiescence proof.
Recovery requires a future ownership-reacquisition or quiescence-proof mechanism;
a restart alone cannot repair this condition.
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

The wire format is strict IPC v5 framing: a four-byte big-endian payload length
followed by one validated message. Unknown fields, wrong message roles, and
oversized frames are rejected.
Partial frame state survives event delivery. Pending `WaitExecution` queries
and PTY input writes run independently of the connection reader, so the same
connection can still query, cancel, release control, or disconnect. PTY input
claims its OperationId before writing; pending and completed retries do not
write again. Other commands are processed in receive order.

## Execution and observation

Submission persists `ExecutionCreated` before scheduling. Once accepted, the
daemon retains responsibility for progress even if the first transition or
work lookup fails. It retries transient store failures independently of the
request connection; replaying a submission is not required to wake it.
The daemon asks the
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
Terminal tasks are removed from memory once their process controls are released.
Their durable projections, facts, and output remain available; fresh cancellation
of a terminal execution returns its stored projection and records an idempotent
response.

`WatchExecution` replays facts after the supplied cursor before forwarding
live facts. Output is addressed by stable `StepId`, stream, and absolute byte
offset. Captured runs expose stdout/stderr; PTY runs expose one terminal stream.
Replay reads every page through a cursor fixed with the stored snapshot, then
filters duplicate live facts at that boundary.

PTY attachments are connection-owned observer leases. At most one attachment
per Step is the controller. Only that controller can write input or resize the
terminal; every attachment may receive the same terminal output stream.
The lease belongs to the individual connection even when another connection
uses the same ClientId. Attachment replay requires the original connection's
registered lease. A durable response without that lease returns OperationExpired;
the client must use a new operation to attach again. EOF, transport errors, and connection task cancellation
release its attachments and controller role. Releasing or losing a controller
lease also cancels its unfinished input, allowing a new controller to proceed.

## Owner boundary

The daemon accepts only fully resolved `ExecutionSpec` values. Surface parsing,
assignment expansion, named sessions, schedules, retry policy, and
approval belong to clients or external producers. Resource admission belongs
to the built-in Composition extension; Core retains its closed algebra. The
daemon provides no v3 compatibility bridge.

Lifecycle commands persist their outcome, then the connection writes and
flushes the response before signalling the host. The host closes admission
and drains owned Runs before releasing its exclusive lock or starting a
successor. No fixed delay substitutes for acknowledgement delivery.

If an acknowledgement is lost before flush, the same host retains the pending
lifecycle outcome across connections. Replaying its OperationId flushes the same
response before signalling once; a successor does not reapply its predecessor's
lifecycle commands.

Drain requests Graceful cancellation for up to five seconds, then Force for up
to five more seconds. If quiescence remains unproven, drain returns an error.
A host exit after that error can leave startup blocked by the attempt marker;
the timeout is not evidence that the processes stopped.

## Local control recovery

The host CLI bounds connection, Hello, and control-response waits. Status keeps
an absent listener distinct from one that accepts connections but cannot speak
IPC v5. Failed Hello reports a protocol-independent recovery command; a lost
control response reports an unknown outcome rather than automatically replaying
or signalling the daemon.

`cued stop --force` is an explicit local escape hatch. It targets only the
same-user PID obtained from the selected Unix socket's kernel credentials,
sends SIGTERM once, and waits up to fifteen seconds for process exit and socket
unavailability. It
rejects non-socket paths, missing peer PID support, and invalid/self PIDs. It
never escalates to SIGKILL or signals a supervisor's replacement. The host
handles SIGTERM through the same drain path as its other shutdown signals.
This facility contains no legacy IPC codec or execution compatibility bridge.

## 资源接入与恢复

The host registers the generic extension dispatcher and transactional submission
effects. It checks extension admission before advancing any leaf, while allowing
cancellation. `cue-resources` owns provider calls and its own SQLite tables.
A serialized background coordinator retries allocation and unresolved cleanup.
Run completion is committed only after the runtime reports physical quiescence;
ownership loss never becomes a terminal fact or release authorization.
Drain stops and joins the coordinator before releasing host ownership. An
interrupted provider call retains its durable uncertain identity for recovery.
See [资源扩展](resources.md) for configuration and failure semantics.
