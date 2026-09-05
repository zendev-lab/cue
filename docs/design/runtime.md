# Cue runtime

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
Core Pipeline, IoMode, StepId, and full Scope. Workspace/transform providers
receive these semantic inputs by shared reference and can only change a separate
SpawnContext containing physical directory placement. It clears the ambient process
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
One PTY input write may be in flight at a time; another input request receives
a busy conflict. Input backpressure does not block resize, process observation,
or termination. Closing a Run interrupts any remaining input and reports a
conflict to that writer; an already written prefix is not rolled back.

## Output and recovery

OutputStore uses absolute monotonically increasing byte offsets. The memory
provider bounds retained bytes per `(StepId, OutputStream)` without resetting
offsets when old bytes are discarded, so readers can detect truncation.
The bundled daemon retains output bytes only for its current process lifetime.
Execution facts, including OutputAppended ranges, survive restart; those bytes
do not. After restart, ReadOutput and TailOutput for an old Step return an empty
buffer. This means no bytes remain in this provider, and does not prove the Run
originally produced no output. Durable output retention needs another provider.

Builtin and Run use the same committed Step lifecycle. `realize_builtin` observes
Cd's resolved directory or reports Env/Umask success; it changes no ambient state
and leaves no physical ownership, so uncommitted builtin completion is replayable.

The host must persist a Run attempt marker before spawning. Unstarted work can be
requeued, but an active marked attempt cannot be replayed or reported terminal
without independent proof of quiescence or unique control. The local provider
cannot regain that control after a crash, so the store/host fail closed. The
runtime does not automatically turn every Running snapshot into an infrastructure
failure. Loss of a runner completion channel reports OwnershipLost, not a false
terminal result.

Cancellation is best effort: natural success and nonzero exit remain normal
completion; a termination signal caused by cancellation reports Cancelled.
