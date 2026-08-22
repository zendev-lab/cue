# Workspace view and SpawnAdapter threat model

Cue has two separate launch mechanisms:

- **workspace view**: Cue's configured overlay/filesystem view;
- **SpawnAdapter**: an optional host-policy decision at the final argv boundary.

The workspace view is not an approval or policy sandbox. Existing wrapper and
overlay behavior remains public and is applied before the adapter.

## Protected assets

- exact final argv and working directory;
- execution output and PTY bytes;
- adapter bearer token;
- daemon database and event stream;
- host files outside the selected workspace view.

## Adapter boundary

For each real pipeline segment, `PrepareSpawn` is called exactly once after
scope/env/resource resolution, argv expansion, workspace view, and wrapper. A
denial returns before `Command::new` can spawn the original command.
`SettleSpawn` receives structured exit/signal/spawn status and a bounded
diagnostic tail; that tail never replaces or truncates canonical pipeline
output.

Socket validation requires a local path inside Cue's private adapter runtime
directory, private permissions, and a same-UID peer. Tokens are opaque, redacted
from debug output, and excluded from environment, persistence, output, and
events.

If prepare is unavailable, launch fails closed. If settle is unavailable, Cue
keeps the raw child result but marks the execution as infrastructure failure.
If a broker disappears before later plan steps, those steps fail closed.

Schedule templates reject adapters because a transient approval or broker lease
cannot authorize future execution. Confined SSH is also rejected until a remote
lease transport has an explicit design.

Host-specific policy, approval, denial signatures, and runner-failure
classification stay outside `cued`. DSH implements them in its temporary
broker; Cue sees only the generic adapter protocol.
