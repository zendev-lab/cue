# Cue architecture

Cue has one execution contract and one daemon protocol:

```text
frontend Scope + surface source
       | PutScope       | compile
       v                v
ScopeHash --------> ExecutionSpec { ScopeHash, closed ExecutionPlan }
                         |
                         v
IPC v4 -> reducer -> typed RuntimeAssembly -> process/PTY/output
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

## 沿实现边界追踪变更

The [interactive code map](docs/codemap/codemap.html) provides clickable ownership,
call/data-flow relationships, production crate dependencies and test declarations.
The [map guide](docs/codemap/README.md) explains local use and evidence validation.

| Boundary | Owner | Input → output |
| --- | --- | --- |
| User intent | CLI / TUI / client | arguments or surface text → explicit Scope and client requests |
| Surface compilation | language | source + Scope hash → closed typed ExecutionSpec |
| Transport contract | protocol | validated IPC v4 messages ↔ length-prefixed JSON |
| Semantic decisions | core | plan + state + completion/cancel → transitions and facts |
| Durable orchestration | daemon + store | command → atomic operation outcome, projection, facts and runtime work |
| Physical effects | runtime | ready leaf + full Scope → owned process/PTY and known completion |
| Observation | daemon + client | stored/live facts and retained bytes → frontend projection |

The daemon commits transitions before publishing or realizing follow-up work.
The reducer does not call a process runner; the daemon interprets its ready
Steps through a typed runtime assembly. The SQLite provider stores output-range
facts, while the runtime's memory output provider stores the actual bytes. This
split matters when diagnosing an empty output response after restart.

For source review, start at `ExecutionClient::submit_file`,
`DaemonService::submit`/`realize`, `Store::commit_execution_command`, and
`LocalProcessSpawner::spawn`. The map links these owning files and their test
locations without treating a build dependency as proof of a runtime call.
