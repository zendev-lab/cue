# Cue vNext execution reducer

This slice turns the closed vNext plan algebra into the sole orchestration
state machine. The daemon persists and projects reducer facts; runtime code may
realize ready work, but it may not choose branches, maintain an independent
scope cursor, or turn an effect intent into a terminal fact before completion.

## Durable leaf state

Every Builtin and Run leaf receives a stable pre-order `StepId` before work
starts. Its durable record contains:

```text
StepRecord = StepId × StepState × input ScopeHash? × output ScopeHash?
StepState  = Pending
           | Running
           | Cancelling(reason, mode)
           | Succeeded
           | Failed
           | Skipped
           | Cancelled
```

`Skipped` means the structured plan selected a leaf out before it started.
`Cancelling` means Core has accepted a cancellation intent but runtime has not
yet reported a terminal completion. `Cancelled` is terminal and means runtime
confirmed cancellation, or the Step was still Pending and therefore had no
external realization to stop.

Cancellation is best-effort. If a Run is already `Cancelling` but runtime later
reports normal success, the reducer records `Succeeded`; a normal failure stays
`Failed`; only an explicit cancellation completion becomes `Cancelled`. Race
semantics depend only on the order in which typed inputs are accepted by the
reducer, never wall-clock timing.

Snapshots contain the plan, leaf records, hashes, and an optional execution
cancellation request. The request means “do not start more work”; it is not a
terminal result. While active work is draining the Execution projects
`Cancelling`, and the eventual terminal state is still derived from actual Step
outcomes and the plan algebra.

Full Scope values remain in the content-addressed `ScopeStore`. Restore rejects
a mismatched leaf count, forged StepId order, active states without input Scope,
and states whose required output hashes are absent.

## Scope transitions

Run always returns its input ScopeHash on normal success or failure. A confirmed
cancelled Run has no output ScopeHash because no normal leaf completion was
observed. Env and Umask are applied as pure Core operations. The runtime only
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

`AnySuccess` distinguishes winner selection from loser cleanup. Once a branch
succeeds, Pending loser leaves become `Skipped`; Running loser Runs become
`Cancelling(AnySuccessSatisfied, Force)` and emit typed cancellation effects.
The Parallel remains non-terminal until all active losers report a terminal
completion. This preserves structured execution: a terminal Parallel never
owns orphan work.

## Runtime boundary

`advance` produces three disjoint outputs:

- ready leaves with typed action and input ScopeHash;
- typed `CancelStep { id, reason, mode }` effect intents for cancellable Running
  Runs;
- newly created Scope values which must be made available before dependent
  ready leaves are realized.

A Running builtin may also enter `Cancelling`, but because the current builtin
realization has no process control effect, Core simply waits for its normal
completion and prevents new work from starting.

The reducer never treats emitting `CancelStep` as proof that the effect
succeeded. Runtime reports one of `RunCompletion::Succeeded`, `Failed`, or
`Cancelled`; `Cancelled` is only valid after a committed cancellation intent.
Repeated cancellation at the same strength is idempotent, while Force may
strengthen an earlier Graceful request and emit a new effect.

Transitions remain pure and transactional inside Core: an invalid restored
Scope relationship returns an error without partially changing reducer state.
The persistence layer added by later stack slices must additionally commit the
next projection, facts, Scope values and effect outbox before publishing state
or dispatching runtime effects.

Daemon restart maps both `Running` and `Cancelling` leaves to an infrastructure
failure. If no execution-wide cancel request exists, the same reducer may then
select a Failure or Always continuation. Later protocol/runtime slices must use
a per-Step launch/control slot so cancellation cannot be lost between durable
Running state and process-control creation.

The following slices add protocol facts, transactional effect persistence, the
captured/PTY runner, and frontend projections around this reducer. They do not
reimplement its branch, cancellation, or Scope semantics.
