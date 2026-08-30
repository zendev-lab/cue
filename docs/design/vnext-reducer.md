# Cue vNext execution reducer

This slice turns the closed vNext plan algebra into the sole orchestration
state machine. The daemon persists and projects reducer facts; runtime code may
realize ready work, but it may not choose branches or maintain an independent
scope cursor.

## Durable leaf state

Every Builtin and Run leaf receives a stable pre-order `StepId` before work
starts. Its durable record contains:

```text
StepRecord = StepId × StepState × input ScopeHash? × output ScopeHash?
StepState  = Pending | Running | Succeeded | Failed | Skipped | Cancelled
```

`Skipped` means the structured plan selected a leaf out before it started.
`Cancelled` means work was explicitly stopped, either by the caller or because
another `AnySuccess` branch won. A condition-selected-out leaf is therefore
Skipped, while an already-running `AnySuccess` loser is Cancelled and returned
to the runtime as process work to terminate.

Snapshots contain the plan, leaf records, hashes, and explicit execution
cancellation reason. Full Scope values remain in the content-addressed
`ScopeStore`. Restore rejects a mismatched leaf count, forged StepId order, and
states whose required input/output hashes are absent.

## Scope transitions

Run always returns its input ScopeHash, regardless of process success or
failure. Env and Umask are applied as pure Core operations. The runtime only
resolves the filesystem-dependent result of Cd and reports an absolute path;
Core then constructs and hashes the resulting Scope.

Sequence passes the terminal ScopeHash from `first` into `then` when its
condition selects `then`. This also means a failure handler sees changes made
before the failure. `Always` runs cleanup after success or failure and fails if
either side fails.

Parallel gives the same input ScopeHash to every branch. Branch-local builtins
may create independent Scope values, but the Parallel node always returns its
original input ScopeHash. There is no merge rule and no daemon `current_scope`
to accidentally leak a branch mutation outward.

## Runtime boundary

`advance` produces three disjoint outputs:

- ready leaves with typed action and input ScopeHash;
- running StepIds which must be terminated;
- newly created Scope values which must be persisted before dependent ready
  leaves are started.

Transitions are transactional: an invalid restored Scope relationship returns
an error without partially changing durable reducer state. Daemon restart maps
previously Running leaves to an infrastructure failure, after which the same
reducer may select a Failure or Always continuation.

The following slices add protocol facts, persistence transactions, the
captured/PTY runner, and frontend projections around this reducer. They do not
reimplement its branch or Scope semantics.
