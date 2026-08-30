# Cue

Cue is a durable local execution kernel for work shared by people and agents.
Clients submit a fully typed `ExecutionSpec`; `cued` owns process groups, PTYs,
output, execution facts, idempotency, and restart recovery.

Cue deliberately does not own session cursors, schedules, automatic retry,
resource policy, approvals, remote fleets, or a general DAG. Those systems may
submit ordinary executions, but cannot extend the closed execution algebra.

## Quick start

Install the Python distribution (the command names remain Cue):

```bash
uv tool install cue-run
```

Start the local daemon in one terminal, then use another terminal:

```bash
cued start

cue client exec "printf hello"
cue client list
cue run examples/hello.cue
cue tui
cue daemon status
```

The installed commands are `cue`, `cue-client`, `cue-tui`, and `cued`.
`CUE_SOCKET` selects a non-default local Unix socket. Remote transport, named
targets, and service management are external wrappers rather than daemon state.

## Execution semantics

`ExecutionPlan` has exactly four variants:

- `Builtin`: `cd`, `env set|unset`, or `umask`;
- `Run`: one typed process pipeline and its captured-or-PTY I/O mode;
- `Sequence`: run the second plan on success, failure, or always;
- `Parallel`: join all branches or finish after any branch succeeds.

Builtin and Run leaves receive stable `StepId` values such as `E7/S2`.
Sequence threads the resulting Scope; Parallel forks one input Scope into every
branch and never merges branch mutations.

The frontend language is direct argv, not an implicit shell:

```cue
RUST_LOG=debug cargo test
printf hello |> wc -c
cargo fmt -> cargo clippy
cargo test || cargo test --doc
cargo test ||| cargo test --doc
cd crates/cue-core -> cargo test
env set MODE=release -> printenv MODE
```

`A=B command` patches only that process. In `A=B left |> right`, the right
process does not inherit `A`. `command A=B` keeps `A=B` as a literal argument,
and an assignment without an executable is rejected. Use the `env` builtin to
change the Scope seen by later sequence steps.

Operators map as follows:

| Surface | Core meaning |
| --- | --- |
| `A \|> B` | stdout to the next process in one Pipeline |
| `A \|&> B` | stdout and stderr to the next process |
| `A \|!> B` | stderr to the next process |
| `A && B`, `A -> B` | Sequence on success |
| `A \|\| B` | Sequence on failure |
| `A ~> B` | Sequence always |
| `A \|\|\| B` | Parallel, all must succeed |
| `A \|?\| B` | Parallel, any success wins |

## IPC v4 and persistence

IPC v4 uses strict length-prefixed JSON on a private Unix socket. Every
connection begins with `Hello`; read-only Queries use `RequestId`, while every
side-effecting Command also carries an idempotent `OperationId`. The protocol
contains explicit Scope, Execution, output, PTY attachment, and daemon
lifecycle operations—no raw source or ambient session handshake.

The default database is `$XDG_DATA_HOME/cue/cued-v4.db` (or the corresponding
XDG fallback). A legacy `cued.db` is renamed to a read-only
`cued-v3-<timestamp>.db.archive` with its sidecars. Cue does not import or
dual-read incompatible v3 semantics. Credential-shaped environments remain
volatile and are not serialized to SQLite.

## CLI

```text
cue-client run FILE.cue
cue-client exec SOURCE
cue-client list
cue-client show|wait E7
cue-client out|err|terminal E7/S2
cue-client cancel|kill E7
cue-client fg E7/S2 [--observe]
cue-client restart|shutdown
```

`cue run` and `cue fg` are shortcuts. PTY control uses one controller and any
number of observers; Ctrl-] detaches the controller CLI.

## Repository structure

- `cue-core`: root execution ADT, Scope, reducer, facts, and identities;
- `cue-protocol`: strict IPC v4 messages and framing;
- `cue-store-sqlite`: Scope/Execution/fact/operation persistence provider;
- `cue-runtime`: bootstrap Composition, typed providers, runner, and recovery;
- `cue-language`: surface tokenizer, parser, compiler, completion, highlighting;
- `cue-daemon`: composition root, IPC service, lifecycle, and local host;
- `cue-client`: explicit Scope submission and sequential/multiplexed clients;
- `cue-tui`: small execution projection;
- `cue-cli`: installed command aggregator and extension dispatch.

Development gates:

```bash
just check
just test
just msrv
just package-smoke
just npm-package-smoke
```

See [architecture](ARCHITECTURE.md), [design](docs/design/README.md),
[testing](docs/testing.md), and the canonical [agent Skill](skills/cue/SKILL.md).
