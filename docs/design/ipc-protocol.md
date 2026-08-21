# IPC v3: `cued` and clients

IPC v3 is a hard, strict typed contract. The daemon does not accept raw Cue
language source. Old clients fail capability/version checks with an actionable
upgrade/restart error.

## Transport and framing

Local transport is a private Unix socket under `$XDG_RUNTIME_DIR/cue/` (or a
private temp fallback). `cued gateway --stdio` relays the identical stream for
an explicitly configured SSH client.

Each message is a four-byte big-endian length followed by UTF-8 JSON. The
maximum frame is 16 MiB. Envelopes and fixed payload structs reject unknown
fields.

```rust
enum Message {
    Request {
        id: u32,
        operation_id: Option<String>,
        payload: RequestPayload,
    },
    Response { id: u32, payload: ResponsePayload },
    Event { payload: EventPayload },
}
```

Request IDs correlate one connection. A non-empty bounded `operation_id` may
be attached only to a side-effecting request after handshake. The daemon
fingerprints the payload, fans out concurrent identical retries, rejects
conflicts, persists completion facts, and never reroutes a completed tombstone.

## Handshake

A client first sends `Ping` to verify protocol version 3 and
`execution-v3`, then sends `Handshake { session_id, cwd, env, refresh }`.
Session-dependent requests sent before handshake fail. Reconnect preserves the
existing cursor; `refresh=true` is an explicit replacement used only when a
volatile sensitive scope could not survive restart.

## Requests

Execution:

- `SubmitExecution { spec }`
- `GetExecution { id }`
- `ListExecutions { limit }`
- `WaitExecution { id }`
- `CancelExecution { id, mode: graceful | force }`
- `ReadExecutionOutput { id, step_id?, stdout_bytes?, stderr_bytes? }`

Scope/session:

- `ApplyScopeDelta`, `GetScope`, `ListScopes`
- `CreateSession`, `AttachSession`, `SessionInfo`
- active/archived/all session lists, archive, and restore

Schedule:

- `CreateSchedule { schedule, execution }`
- `ListSchedules`, `PauseSchedule`, `ResumeSchedule`, `RemoveSchedule`

PTY step:

- `StepAttach { id }`, `StepWatch { id }`
- `StepClaimControl`, `StepReleaseControl`, `StepDetach`
- `StepInput { data }`, `StepResize { cols, rows }`

System and inspection:

- `Subscribe`, `Unsubscribe`, `ListResources`, `ShowEnv`, `ShowConfig`
- `Ping`, `Restart`, `Shutdown`

`Eval`, `RunScript`, `Complete`, `Highlight`, `KillJob`, and competing
job/chain/script query APIs do not exist in v3.

## Responses and events

Responses are `Ok(typed payload)` or `Err { code, message }`. Execution
responses contain `ExecutionInfo`, stable step projections, or bounded output.
PTY attachment snapshots and all binary chunks are base64 strings in JSON.

Execution lifecycle events are:

- `ExecutionCreated`
- `ExecutionStateChanged`
- `StepStateChanged`
- `OutputChunk { id: StepId, stream, data }`
- `ExecutionFinished`

PTY observers additionally receive attachment-scoped `FgOutput`,
`FgControlChanged`, and `FgExited`. These carry both `StepId` and
`attachment_id`, so delayed messages cannot mutate a newer attachment.

Subscription channels are the closed set `executions`, `scopes`, and
`system`. There are no per-job output channels or EOF mirror events.

## Compatibility

Protocol v3 is not dual-stack. Capabilities gate typed clients before they
allocate a request ID or write an unsupported request. The daemon retains no
server-side conversion bridge from raw language source or J/CH/R identifiers.
