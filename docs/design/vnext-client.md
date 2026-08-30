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
environment a command came from. Sensitive environment names are accepted by
the client but classified by the daemon as volatile and are never persisted.

## Identity and framing

A connection creates one bounded `ClientId` and sends `Hello` before any other
message. Every query receives a fresh non-zero `RequestId`; every command also
receives a fresh `OperationId` containing a per-connection random prefix. The
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
