# Cue vNext runtime

The runtime realizes ready Core leaves without owning execution semantics. It
has no parser, IPC request, session cursor, schedule, resource policy, retry
loop, or dynamic service lookup.

## Typed Assembly

Bootstrap first resolves provider metadata with the Composition combine laws,
then consumes a `ProviderRegistry` into one `RuntimeAssembly`. The resulting
root contains typed fields for ExecutionStore, ScopeStore, OutputStore, and
ProcessSpawner, plus ordered Workspace, SpawnTransform, SpawnGuard, and
ExecutionObserver implementations. A resolved provider without the matching
typed implementation fails binding before admission.

Execution code receives `RuntimeAssembly`; it never asks for a provider by
string. The retained manifest is non-sensitive inspection data only.

## Local process runner

`LocalProcessSpawner` takes an already resolved `SpawnRequest` containing a
Core Pipeline, IoMode, StepId, and full Scope. It clears the ambient process
environment, applies each Process-local EnvPatch independently, sets cwd and
umask, and spawns every pipeline segment directly without an implicit shell.

Captured mode preserves each typed PipeLink and stores every unlinked stdout
or stderr stream separately. Pipeline success follows the final process after
all segments are reaped. Every segment owns a process group; partial-spawn
failure, cancellation, and normal leader exit clean descendant processes.

PTY mode retains the same internal PipeLinks but routes all terminal-facing
stdin/stdout/stderr through one PTY endpoint for the entire Run. Its RunControl
accepts explicit input, resize, graceful termination, and force termination;
captured runs reject terminal input and resize.

## Output and recovery

OutputStore uses absolute monotonically increasing byte offsets. The memory
provider bounds retained bytes per `(StepId, OutputStream)` without resetting
offsets when old bytes are discarded, so readers can detect truncation.

On daemon restart, `recover_interrupted` converts only previously Running
Steps into structured infrastructure failures and emits the matching ordered
facts. It does not advance pending branches in the same function: recovery is
persisted first, then the normal reducer advance/mark-running transaction may
start a Failure or Always continuation.
