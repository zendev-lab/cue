# Cue

Cue is a persistent, observable local execution runtime for work shared by
humans and agents. `cued` owns process, PTY, scope, resource, schedule, output,
and recovery state; clients submit one typed `ExecutionSpec` and observe one
`ExecutionId` (`E<n>`) with stable process-step IDs (`E<n>/S<n>`).

> **Pre-1.0:** IPC v3 is an intentional hard cut. Older `Eval`, `RunScript`,
> job, chain, and script-state clients are rejected during capability checks.

Cue is not a general shell, workflow engine, fleet manager, or policy daemon.
Its shell-like language remains a frontend convenience: `cue-language` compiles
interactive input and `.cue` files locally into the same typed execution
contract used by non-language clients.

## What Cue owns

- durable executions, steps, named sessions, scopes, schedules, and idempotency facts;
- process groups, resource admission, workspace views, wrappers, PTYs, output, and events;
- multiple PTY observers with one explicit controller lease;
- disconnect-safe background work and restart recovery;
- a constrained per-spawn adapter seam for host policy enforcement.

Higher layers own agent/workflow policy, approvals, secrets, remote-fleet
coordination, and general DAG/retry semantics. In particular, `cued` has no DSH
mode or DSH dependency: DSH confinement is supplied by `dsh-tool-cue` through a
short-lived SpawnAdapter broker.

## Agent skill

[`skills/cue/SKILL.md`](skills/cue/SKILL.md) is the canonical agent-facing Cue
Skill. Cue binaries do not load it. Node hosts install `@zendev-lab/cue` and
mount the package's exported `cueSkillsRoot`; the published package contains
this authority directly rather than a downstream source copy.

## Install and commands

The Python distribution is `cue-run`; the product and commands remain Cue:

```bash
uv tool install cue-run

cue --help
cue tui
cue run build.cue
cue client session list --json
cue daemon status
cued --version
```

The installed command set is `cue`, `cue-client`, `cue-tui`, and `cued`.
`cue daemon ...` forwards to `cued`; there is no standalone `cue-daemon`
command alias. Because Google CUE also installs a `cue` command, installation
diagnostics detect a foreign binary rather than assuming it is this runtime.

## Language frontend

Commands are direct argv, not implicit `/bin/sh` strings:

```cue
cargo test
RUST_LOG=debug cargo test
MODE=release ./scripts/build
printf hello |> wc -c
cargo fmt -> cargo clippy
cargo test ||| cargo test --doc
```

Leading `NAME=value` words apply only to that pipeline segment. They are typed
environment overrides and never mutate the session scope. Assignment-shaped
arguments after the executable remain ordinary argv.

Composition maps directly to `ExecutionPlan`:

| Language | Typed node | Meaning |
| --- | --- | --- |
| `A |> B` | one `Pipeline` | connect exact argv segments with a pipe |
| `A && B`, `A -> B` | `OnSuccess` | run B only after A succeeds |
| `A || B` | `OnFailure` | run B only after A fails |
| `A ~> B` | `Always` | run B after either result |
| `A ||| B` | `ParallelAll` | run all branches; all must succeed |
| `A |?| B` | `AnySuccess` | succeed after the first successful branch |

A `.cue` file is parsed and compiled by the client into one fail-fast execution
with source metadata:

```bash
cue run scripts/build.cue
```

Retry is deliberately a new submission with `retry_of`; an old execution is
never revived or assigned a second lifecycle.

## Runtime contract

Execution state is exactly `queued | running | succeeded | failed | cancelled`.
A forced stop is `cancelled` with a forced reason. Each actual pipeline segment
has one stable `StepId`; PTY attach/watch/control and output address that step.

IPC v3 uses length-prefixed strict JSON over a local Unix socket or the same
framing through `cued gateway --stdio` for explicit SSH profiles. Core requests
are:

- `SubmitExecution`, `GetExecution`, `ListExecutions`, `WaitExecution`;
- `CancelExecution { graceful | force }`, `ReadExecutionOutput`;
- typed scope/session and schedule operations;
- typed step PTY attach/watch/control operations.

Execution events are `ExecutionCreated`, `ExecutionStateChanged`,
`StepStateChanged`, `OutputChunk`, and `ExecutionFinished`. PTY attachment
lifecycle events remain a separate typed control stream.

See [IPC protocol](docs/design/ipc-protocol.md), [core types](docs/design/core-types.md),
and [daemon architecture](docs/design/daemon-architecture.md).

## Spawn preparation and policy adapters

Every process segment passes through one preparation path, in this order:

1. scope, environment, and resource admission;
2. argv expansion;
3. workspace view;
4. configured wrapper;
5. optional SpawnAdapter `PrepareSpawn`;
6. `Command::new` and spawn.

`SettleSpawn` receives the process result and a bounded diagnostic stderr/PTY
tail. An unavailable adapter fails closed before spawn; an unavailable settle
call preserves the process result but finishes the execution as an
infrastructure failure. Adapter sockets must be private, same-UID Unix sockets
inside Cue's runtime adapter directory. Opaque tokens are not placed in the
environment, database, output, or events. Schedule templates cannot persist an
ephemeral adapter.

## Files and migration

Cue uses standard XDG roots under `cue`:

- config: `$XDG_CONFIG_HOME/cue/`;
- data/database/output: `$XDG_DATA_HOME/cue/`;
- state/logs: `$XDG_STATE_HOME/cue/`;
- runtime/socket/adapters: `$XDG_RUNTIME_DIR/cue/`.

On first upgraded startup, the migration takes the instance lock, rejects
symlinks, archives the legacy `cue-shell` v18 database and output read-only,
and imports only safe session/scope/config context. Legacy J/CH/R history and
cron records are not dual-read or imported. The migration is idempotent and
leaves the old data untouched if publication or import fails.

## Development

```bash
just check
just test
just msrv
just package-smoke
just pre-commit
```

The workspace is split by responsibility:

- `cue-core`: IPC v3 and typed execution/scope/schedule state;
- `cue-language`: tokenizer, parser, resolver, compiler, completion, highlighting;
- `cue-daemon`: the single execution/session/resource/PTY/persistence owner;
- `cue-client`: transport, reconnect, SSH, version checks, and daemon lifecycle;
- `cue-tui`: an interactive client built on `cue-client` and `cue-language`;
- `cue-cli`: the installed command aggregator and Maturin companion binaries.

See [testing](docs/testing.md) and the [design index](docs/design/README.md).
