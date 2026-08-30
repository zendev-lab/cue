---
name: cue
description: |
  Use when Cue is the active local execution backend and work should run
  through its typed execution, output, cancellation, PTY, or .cue file tools.
---

# Cue

Cue is a direct-exec local process kernel, not a Bash-compatible shell. Use it
when durable execution identity, structured composition, later output reads, or
PTY reattachment matter.

## Public commands

| Need | Command |
| --- | --- |
| Run one Cue expression and wait | `cue client exec SOURCE` |
| Run a `.cue` file and wait | `cue run FILE.cue` |
| List or inspect executions | `cue client list`, `cue client show E7` |
| Wait for an execution | `cue client wait E7` |
| Read one Step stream | `cue client out|err|terminal E7/S2` |
| Stop an execution | `cue client cancel E7` or `cue client kill E7` |
| Attach a PTY | `cue fg E7/S2 [--observe]` |
| Inspect daemon health | `cue daemon status` |

The active command help is authoritative. `CUE_SOCKET` may select a local Unix
socket supplied by the host. Cue does not own named targets or SSH profiles.

## Compose direct commands

Cue executes argv directly. It does not expand globs, substitutions, redirects,
heredocs, or arbitrary shell syntax.

| Operator | Meaning |
| --- | --- |
| `\|>`, `\|&>`, `\|!>` | typed pipeline links |
| `&&` or `->` | run right after success |
| `\|\|` | run right after failure |
| `~>` | always run right |
| `\|\|\|` | run in parallel; all must succeed |
| `\|?\|` | run in parallel; first success wins |

Use `cd PATH -> command`, `env set A=B -> command`, and `umask 027 -> command`
for persistent Scope changes within one execution. A prefix assignment is
process-local: `A=B left |> right` changes only `left`; `command A=B` passes a
literal argument; `A=B` alone is invalid.

## Boundaries and recovery

- Use PTY only for software that requires terminal semantics. Ctrl-] detaches
  an interactive controller; `--observe` never claims input control.
- Keep returned `ExecutionId` and `StepId` values. Client disconnect does not
  cancel daemon work.
- Read the relevant Step stream instead of rerunning a failed side effect.
- Schedule, retry, resource selection, approval, remote transport, and session
  policy belong to the calling host or an external producer, not Cue commands.
- For complex shell behavior, write a script and invoke its interpreter as
  explicit argv; do not smuggle an unreviewed command string into Cue.
