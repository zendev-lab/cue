# vNext client

The vNext client owns frontend context and transport correlation. It does not
restore the v3 session cursor or send surface source to the daemon.

## Explicit submission

For each submission the client snapshots the frontend process `cwd`, Unicode
environment, and current `umask` into a complete `Scope`. The ordered flow is:

1. `PutScope(scope)` and receive its content hash plus durability class.
2. Compile surface source with that explicit `ScopeHash`.
3. `SubmitExecution(typed_spec)`.

The daemon therefore never guesses which shell, terminal, session, or process
environment a command came from. Environment values carry explicit sensitivity. The SQLite host rejects
Sensitive values; ordinary variable names do not imply sensitivity.

## Identity and framing

A connection creates one bounded `ClientId` and sends `Hello` before any other
message. Every query receives a fresh non-zero `RequestId`. A logical command owns an
immutable `PreparedCommand` with its ClientId, OperationId, and payload. Socket
reconnection retries it with the original identity. Stream and multiplexed
callers can retain and reuse prepared commands across connection replacement. The
client uses the same strict length-prefixed IPC v4 framing as the daemon and
surfaces typed protocol errors without retrying a rejected effect.

The sequential client buffers events that arrive while it waits for a matching
response. Interactive frontends convert it to `VnextMultiplexedClient`, whose
single reader routes responses to concurrent callers by `RequestId` and sends
facts/PTY events to a separate event queue.

## Language boundary

`cue-language` returns `VnextCommand`; the client is the only layer that maps
those intents into Query or Command envelopes. Schedule, retry, resource,
session, and approval policies remain external owners and cannot reappear as
hidden daemon request fields.

Surface queries compile against a locally computed ScopeHash without PutScope.
Only submission writes Scope. `tail N` uses TailOutput to read the final N
retained bytes and returns their absolute offsets.
