# Cue architecture

Cue has one execution contract and one daemon protocol:

```text
frontend Scope + surface source
       | PutScope       | compile
       v                v
ScopeHash --------> ExecutionSpec { ScopeHash, closed ExecutionPlan }
                         |
                         v
IPC v5 -> extension admission -> reducer -> typed RuntimeAssembly -> process/PTY/output
```

`cue-core` alone decides plan meaning, stable Steps, result aggregation, and
Scope propagation. `cue-runtime` resolves providers at daemon bootstrap and
binds a typed assembly; the execution hot path never performs service lookup.
`cue-daemon` persists reducer transitions and realizes only ready leaves.

Language syntax, session state, schedules, retries, resource policy, approvals,
secrets, remote fleets, and agent workflows are outside the kernel. The daemon
accepts typed values and never parses Cue source.

There is no IPC v3 implementation or dual stack. The only legacy behavior is
read-only archival of the old database before the v4 store opens.

Start with the [design index](docs/design/README.md) and
[project direction](VISION.md) and [design principles](PRINCIPLES.md).

[FP-0001](fps/FP-0001-structured-execution-kernel.md) owns the normative contract.

`cue-resources` is a built-in Composition extension with independent SQLite
tables. Generic IPC extension dispatch, atomic submission effects, admission,
and background cleanup remain host mechanisms. Resource types never enter
Core, protocol variants, or runtime policy. Physical environment contributions
flow through SpawnContext after logical environment construction.
