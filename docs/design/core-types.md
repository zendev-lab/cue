# Core execution types

`cue-core` owns the language-neutral contract and pure execution reducer. It
does not own tokenizer, parser, completion, highlighting, or an interactive
command table.

## Identity

- `ExecutionId`: one immutable submission, displayed as `E<n>`.
- `StepId`: one process-bearing pipeline leaf, displayed as `E<n>/S<n>`.
- `ScheduleId`: one trigger template, displayed as `C<n>`.
- `ScopeHash`: a content-addressed environment and working-directory snapshot.

Retries submit a new `ExecutionSpec` with `retry_of` pointing at the old ID.
No API revives or rewrites a terminal execution.

## ExecutionSpec

```rust
struct ExecutionSpec {
    plan: ExecutionPlan,
    start_scope: Option<ScopeHash>,
    launch_context: LaunchContext,
    source: Option<SourceMetadata>,
    retry_of: Option<ExecutionId>,
}
```

`LaunchContext` contains PTY preference, resource needs, optional workspace
view, wrapper override, and an ephemeral SpawnAdapter handle. The adapter token
is removed before persistence. A scheduled template may not contain one.

## ExecutionPlan

The plan is a deliberately small tree:

```rust
enum ExecutionPlan {
    Pipeline { pipeline: Pipeline },
    OnSuccess { left, right },
    OnFailure { left, right },
    Always { left, right },
    ParallelAll { branches },
    AnySuccess { branches },
    ContextDelta { delta },
}
```

A `Pipeline` preserves exact argv segments, per-segment environment overrides,
and pipe connections. Only process-bearing pipeline nodes receive public step
IDs. Context deltas participate in reducer ordering without pretending to be
processes.

## State

Execution state is:

- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled { user | forced }`

Step state is queued, running, succeeded, failed with an exit/signal/spawn/
infrastructure reason, or cancelled with an explicit reason. Conditional
branches skipped by the reducer are terminal cancelled steps; they never become
unowned shadow jobs.

The reducer is the only owner of composition semantics. The daemon coordinator
supplies node outcomes and performs the returned actions; it does not implement
a second chain or script state machine.
