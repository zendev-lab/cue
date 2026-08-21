# Cue TUI

`cue-tui` is an interactive projection of the typed runtime. It shares
transport, version gating, reconnect, SSH, named-session binding, and daemon
lifecycle code with `cue-client`; it does not maintain a second daemon-control
implementation.

## Main behavior

- input is compiled locally by `cue-language`;
- execution cards are keyed by `ExecutionId`;
- output chunks are routed from `StepId` to their parent execution card;
- early output is buffered until the corresponding execution projection arrives;
- schedules and sessions are rendered from typed list responses;
- reconnect refreshes authoritative projections rather than replaying inferred UI state.

The display pane contains user-selected previews. It has no per-job
subscribe/unsubscribe state machine and no duplicate output EOF protocol.

## PTY sharing

A concrete step can have multiple observers and one controller. Attach/watch
returns a snapshot plus an `attachment_id`. All later PTY events carry the same
`StepId` and attachment epoch; delayed events from an old attachment are
ignored.

- `Ctrl+]`: claim or release controller lease;
- `Ctrl+Z`: detach without stopping the execution;
- `Ctrl+Y`: copy visible terminal contents;
- disconnect: release the controller lease, preserve the process.

Input and resize are forwarded only after controller ownership is confirmed.
A client missing `execution-v3` shows an upgrade/restart error and sends no
typed request to the old daemon.
