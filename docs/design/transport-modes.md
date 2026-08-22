# Client transports

`cue-client` is the sole transport and daemon-lifecycle owner used by CLI and
TUI.

## Local Unix

The default profile connects to Cue's private runtime socket. The client verifies
protocol version, `execution-v3`, daemon readiness, and session-handshake
capability before sending typed work. Local lifecycle commands resolve only
`cued`; `cue daemon ...` is an aggregator spelling.

## Explicit SSH

An SSH profile runs a configured `cued gateway --stdio` command and relays the
same framed IPC stream. The client owns OpenSSH process startup, bounded stderr
diagnostics, reconnect, version checks, and explicit remote start instructions.

The runtime does not discover or manage a fleet. Host discovery can add client
profiles, but it cannot change execution ownership or infer daemon state.

SpawnAdapter confinement over SSH is rejected in the first version because its
endpoint is a local same-UID Unix lease. Direct filesystem-access execution
without a confinement adapter remains available where the host policy allows it.

## Reconnect

Named sessions provide a stable logical context across transport reconnects.
Ordinary reconnect preserves the daemon cursor. Explicit refresh replaces only
a confirmed `needs_refresh` cursor after volatile sensitive environment state
was intentionally not persisted.

Disconnect never means execution completion. Clients use typed execution
queries/waits and output reads after reconnect.
