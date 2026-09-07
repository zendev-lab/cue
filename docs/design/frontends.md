# Cue executable frontends

The `cue-client` binary, top-level `cue` aggregator, and `cue-tui` now present
only IPC v4 execution concepts. Stable user identities are `ExecutionId` and
`StepId`; J/CH/R identifiers and session attachment epochs are absent.

## CLI

The client offers file and one-line submission, list/show/wait, per-Step
stdout/stderr/terminal reads, graceful/forced cancellation, PTY attach, and
daemon lifecycle commands. PTY attach replays the terminal tail, optionally
claims the sole controller lease, forwards raw input, and detaches on Ctrl-].
Pending input acknowledgements do not block terminal events, EOF, or detach.
Unsent input is buffered up to 64 KiB; exceeding that bound detaches with an
explicit error rather than silently discarding bytes. Blocking terminal reads
run outside the async runtime so an idle stdin cannot delay process exit.

File and one-line runners wait for completion before printing retained bytes;
they do not yet provide live output or stdin forwarding. Non-process failures
include the failed Step and stored reason on stderr. Reads from offset zero
warn when the provider returns a later retained offset. The default retention
is 1 MiB per stream and is lost on daemon restart.

`cue run` and `cue fg` are direct shortcuts. Session, schedule, retry,
resource, target, and approval commands are not builtin namespaces; an
external producer may still be installed through the extension mechanism.

## TUI

The TUI is an execution projection rather than a second workflow engine. It
shows recent v4 `ExecutionView` values, accepts the shared Cue surface
language, submits through explicit Scope values, watches facts, and refreshes
from authoritative daemon projections. Output and typed errors are kept in a
small activity log. PTY terminal emulation remains the CLI passthrough owner's
job, so `:fg E1/S1` points to `cue fg E1/S1`.
The serial and multiplexed clients share the surface-to-protocol mapping.
Compilation is local, only submissions persist Scope, and tail requests use
TailOutput. Waits have a separate bounded queue from ordinary commands, so a
full wait queue still permits cancellation. Requests and coalesced list refreshes
run outside the TUI event loop, keeping editing and quit keys responsive while
network responses are pending.

Only executions submitted by this TUI connection are automatically watched.
Use `:jobs` to refresh work submitted elsewhere or executions that were already
running when the TUI opened. There is currently no global live list subscription
or periodic refresh. Each submission snapshots the frontend's original process
context; a standalone `cd`/`env` does not move the next prompt's context. Compose
such changes with the command in one execution.

The former session/cron/resource pages, client-side v3 state machine, target
modal, foreground epoch compatibility, and debug protocol were deleted rather
than hidden behind flags.

This is a reduced TUI, not feature parity with the previous interface. The old
card interactions, sidebar/detail layout, completion UI, and clipboard actions
have not been ported. Shared language completion support does not imply an
interactive completion menu. Restoring these frontend features can use the v4
projections without restoring the deleted v3 workflow owners. The v4 cut ships
this reduced interface; frontend feature parity remains follow-up work.
