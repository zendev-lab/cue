# Cue vNext execution reducer

[FP-0001](../../fps/FP-0001-structured-execution-kernel.md) defines the public
contract. The reducer is the only owner of branch selection, Scope propagation,
cancellation, and the derived Execution state.

Every Builtin and Run receives a stable StepId in plan preorder. `advance()`
atomically sets eligible Pending leaves to Running, writes their input ScopeHash,
and includes their IDs in `ExecutionTransition.runtime_steps`. Calling it again
does not emit the same work. Runtime resolves each ID against the latest committed
snapshot; no ready/cancel payload or external `mark_running()` phase exists.

Cancellation records `StepCancelCause` independently of `CancelMode`. The first
accepted cause is retained and mode can only strengthen from Graceful to Force.
Running becomes Cancelling and remains active until typed completion. Normal
success or failure can win the race; cancellation completion is accepted only
for a Cancelling Step. The same lifecycle applies to builtins, including direct
cancellation before realization. Execution snapshots store only the optional
execution cancellation mode.

AnySuccess excludes Pending losers and requests Force cancellation of active
losers before advancing any continuation. It waits for every active loser to
terminate. Execution cancellation and ancestor loser exclusion prevent starting
Failure/Always successors even if cancelled work subsequently succeeds. Always
preserves failures; an entirely skipped successor contributes no result or Scope.

Run preserves its input Scope on success or failure. Successful Env/Umask reports
carry no replacement Scope; Core applies their command to the input. Cd reports
only a resolved absolute directory. Failed builtins preserve input Scope and
cancelled leaves produce no output. Sequence threads the executed path's Scope;
Parallel forks and returns its input without merging branches.

Restore checks plan cardinality, Step identity, field presence, cancellation
consistency, control-flow eligibility, and Scope relationships. Invalid reducer
inputs leave the prior state intact. Restart interruption applies only to active
Runs after the host has independently proved their old attempts quiescent;
builtins can safely be realized again from their committed input.

The store must make new Scope values durable and atomically commit snapshot,
facts, and StepId follow-up before updating live state or publishing facts.
Delivery generations and process ownership belong to the implementation layers.
