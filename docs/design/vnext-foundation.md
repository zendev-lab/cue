# Cue vNext foundation

This document describes the foundation used to migrate the current IPC v3
runtime. [FP-0001](../../fps/FP-0001-structured-execution-kernel.md) owns the
public contract; later stack layers implement its reducer, protocol, storage,
runtime, and clients.

## Product boundary

Cue is the durable local owner of one finite structured execution. It persists
Execution/Step facts, manages process groups, captured output and PTY control,
and preserves operation idempotency across reconnects and restarts.

The kernel does not own a session cursor, schedule timer, automatic retry,
resource scheduling policy, approval workflow, agent concept, remote fleet, or
general DAG. Those systems submit new executions through an
`ExecutionSubmitter` and keep their own state.

## Closed execution semantics

The target `cue_core::vnext` algebra has four variants:

```rust
enum ExecutionPlan {
    Builtin { command: BuiltinCommand },
    Run { pipeline: Pipeline, io: IoMode },
    Sequence { first, then, when: SequenceCondition },
    Parallel { branches: ParallelBranches, join: ParallelJoin },
}
```

Builtin and Run leaves are observable Steps. Sequence and Parallel organize
those Steps but do not receive independent process identities. Step IDs are
allocated in stable pre-order before execution begins.

The structure rejects empty argv, empty pipelines, dangling pipe links, empty
Env builtins, and parallel compositions with fewer than two branches. The IPC
boundary must construct the same smart types rather than deserializing into a
second unchecked representation.

`ExecutionSpec.scope` is mandatory. Run leaves never change Scope. Builtin may
produce a new Scope; Sequence passes it forward. Every Parallel branch starts
from the same input Scope, and the Parallel result always returns that input
Scope rather than merging branch mutations.

## Scope and process data

Scope is the full immutable snapshot:

```text
Scope = absolute cwd × Env × umask
```

Each environment value carries an explicit Normal or Sensitive classification.
Its identity uses a versioned canonical hash that includes that classification. Parent/delta relationships may be
recorded as execution history, but are not part of Scope identity.

The Core builtin families are Cd, Env, and Umask. Env uses a map from EnvKey to
`Set(value) | Unset`, so one key cannot be set and unset simultaneously while a
single mutation can edit different keys in both directions.

A Pipeline is represented as one first Process followed by linked
continuations. Each continuation owns both its PipeLink and successor Process;
there are no parallel `processes` and `links` arrays to validate. A Process
EnvPatch applies only to that Process's copy of the input environment.

The surface assignment rule is separate from Core environment validity:

```text
A=B command arg         -> Process.env[A] = Set("B")
A=B left |> right       -> only left receives A=B
command A=B             -> "A=B" is a literal argv word
env set A=B -> command  -> Builtin Env changes the Scope seen by command
```

Assignments are recognized only in the prefix before a pipeline segment's
first command word. Multiple prefix assignments are allowed, `A=` sets an
empty value, and an assignment prefix without a command is rejected. The
surface identifier grammar remains shell-like; Core `EnvKey` instead accepts
any non-empty OS-compatible UTF-8 name without `=` or NUL. Persistent unset is
an Env builtin edit; a future process-local unset spelling compiles to the same
Process EnvPatch rather than adding Core syntax.

PTY is resolved per Run. Captured and Pty are the only Core modes; heuristics
and `Auto` belong to the frontend. A Pty Pipeline has one terminal-facing
endpoint while its internal PipeLinks remain intact.

## Bootstrap Composition

`cue-runtime` resolves an open provider graph before daemon readiness. The
canonical ports and combine laws are:

| Port | Combine |
| --- | --- |
| ExecutionStore | ExactlyOne |
| ScopeStore | ExactlyOne |
| OutputStore | ExactlyOne |
| ProcessSpawner | ExactlyOne |
| Workspace | ZeroOrOne |
| SpawnTransform | Chain |
| SpawnGuard | All |
| ExecutionObserver | Fanout |

Extensions may add private ports used by their own providers. Providers depend
on ports rather than named implementations. Missing, ambiguous, unknown, or
cyclic dependencies fail composition before the daemon opens admission.
Multi-provider ports have a deterministic topological order. Before/after
constraints name the port contribution explicitly; ordering one port neither
requires contributions to other ports nor changes global initialization order.
Provider initialization follows required-port dependencies.

The resolved Assembly is only a bootstrap artifact. The daemon converts it to
typed runtime fields and retains a non-sensitive manifest for inspection; the
execution hot path must not use a service locator.

## Migration rule

During development, the current root modules continue to serve IPC v3 while
new code is built under the explicit `vnext` namespace. New daemon behavior
must not partially translate between the two contracts. Once language,
protocol, storage, runtime, and clients all use vNext, the old modules are
deleted and vNext becomes the root API in one hard cut.
