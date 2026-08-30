# Cue vNext protocol and store

IPC v4 is a strict transport projection of the vNext Core contract. It lives in
`cue-protocol`; `cue-core` never depends on transport. The current IPC v3 types
remain reachable only by the binaries being migrated and are deleted at the
hard cut.

## Message boundary

Every frame is exact four-byte big-endian length plus strict JSON. Missing,
truncated, oversized, trailing, or unknown data is rejected. Binary output and
PTY data use base64 strings rather than JSON integer arrays.

The envelope separates read-only and side-effecting traffic:

```text
Query   = RequestId × Query
Command = RequestId × OperationId × Command
```

A Query cannot carry an OperationId and its variants do not mutate durable or
connection state. Every Command must carry one. `Hello` establishes a stable
ClientId for the connection, so the durable at-most-once key is
`(ClientId, OperationId)` rather than the removed session namespace.

The v4 request surface owns only Scope upload/query, execution
submit/query/list/wait/cancel/watch, output ranges, explicit PTY attachments,
and daemon lifecycle. It has no session cursor, schedule, resource admission,
retry policy, raw source, or ambient cwd/env handshake fields.

## Facts and live events

Committed execution changes receive a monotonic EventId. Durable facts include
execution creation, Step and Execution state changes, output offset ranges, and
terminal execution state. Step facts carry the same input/output ScopeHash as
the reducer record. Event replay is ordered by `(ExecutionId, EventId)`.

Output bytes live in OutputStore and facts record only append offsets. PTY bytes,
attachment role changes, detach notices, and daemon draining notices are live
events. An AttachmentId is mandatory for every PTY input, resize, control, and
detach command; no connection-wide implicit "foreground Step" exists.

## Idempotency

`cue-store-sqlite` hashes ClientId and OperationId under separate versioned
domains and fingerprints canonical typed Command JSON. The first completed
operation stores its response. Reusing the key with the same fingerprint
replays that response; reusing it with a different fingerprint is a conflict.
Retention may discard an old replay payload, but keeps a permanent tombstone so
the side effect is never routed a second time.

## Fresh persistence schema

The provider owns four tables only:

- content-addressed full Scope snapshots;
- complete reducer Execution snapshots and aggregate state;
- ordered durable facts;
- operation replay records/tombstones.

Execution projection and its facts commit in one SQLite transaction. For a
Command, the operation claim, replay response, projection, and facts share that
same transaction. The store reconstructs the Core reducer on every read and
requires facts to transform the previous Step/Execution projection into the
new one in order; a fact cannot invent its previous state or omit a durable
state change. Scope rows are rehashed on read, and every ScopeHash referenced
by a durable execution must exist in the same store.

Credential-shaped environment keys make a Scope volatile: it remains eligible
for the daemon's memory cache but is never serialized to SQLite. Consequently,
an execution that references such a Scope is also volatile and cannot be
committed as a restart-recoverable execution.

This is a new schema, not a v22 extension of the IPC v3 database. At the hard
cut the daemon creates the vNext store and archives the old database read-only;
there is no semantic import or dual-read path for session, schedule, resource,
or v3 execution rows.
