# Sandbox Threat Model

`cue-shell` can run a job inside an **overlay workspace sandbox** via
`:run(sandbox=overlay)`. This document states precisely what that sandbox does
and does not protect against, so the `sandbox=` name is never mistaken for a
security boundary.

> **Summary**: the overlay sandbox is a *copy-on-write workspace view*. It keeps
> a job's writes out of the real working tree. It is **not** a process, network,
> credential, or syscall isolation boundary.

## What it is

When a job opts into `sandbox=overlay`, the daemon mounts a Linux `overlayfs`
whose lower layer is the job's (canonicalized) working directory and whose
upper/work layers live outside the working tree:

```text
lowerdir = <canonical job cwd>            (read-through)
upperdir = <upper-root>/<job-id>/upper    (writes land here)
workdir  = <upper-root>/<job-id>/work     (overlayfs scratch)
merged   = <runtime>/sandbox/<job-id>/merged   (the job's cwd)
```

The job runs with its cwd rewritten to `merged`. Reads see the real tree;
writes are redirected into the per-job upper layer and never modify the lower
tree. When the job's sandbox is dropped, the overlay is unmounted and the
per-job upper/work directories are removed.

### Per-job isolation

Every job derives an **independent** upper/work pair under the upper root:

- Default upper root: `[sandbox] default_upper_root` (default
  `/dev/shm/cue-shell-sandbox`, i.e. shared memory / tmpfs).
- `sandbox.upper=<dir>` is treated as an **upper root**, not a literal
  `upperdir`. The daemon still appends `/<job-id>/{upper,work}`, so two jobs
  pointed at the same `<dir>` never share one upper layer.
- `sandbox.upper=tmpfs` mounts a fresh private tmpfs for that single job's
  upper/work.

This means concurrent jobs cannot observe or corrupt each other's overlay
writes, and a job's writes are discarded on completion.

### Shared-memory guard

Because the default upper root is tmpfs (`/dev/shm`), uncontrolled writes would
consume host RAM. Before preparing a sandbox the daemon checks the free-space
ratio of the upper-root filesystem and refuses to start when it is below
`[sandbox] min_free_ratio` (default `0.1`). Set the ratio to `0.0` to disable
the guard, or point `default_upper_root` at a disk-backed filesystem.

## Trust boundaries

| Actor / input | Trust | Notes |
|---|---|---|
| The command being run | **untrusted for the working tree only** | Its writes are contained to the overlay; everything else runs with the daemon's privileges. |
| Absolute paths outside cwd (`/etc`, `$HOME`, `/var`, ...) | **not isolated** | The process can read and write them exactly as the daemon user can. |
| Network | **not isolated** | The job shares the daemon's network namespace and can reach anything the host can. |
| Process credentials / uid / capabilities | **not isolated** | No user namespace, no privilege drop. |
| Inherited environment | **not isolated** | The resolved scope env is passed through; secrets in env remain visible. |
| Other processes / signals | **not isolated** | No PID namespace; the job can see and signal host processes per OS permissions. |
| Daemon config (`daemon.toml`) | **trusted** | Operator-provided; controls upper root, guards, block/warn rules. |

## What it does NOT protect against

The overlay sandbox does **not**:

- Prevent reads or writes to absolute paths outside the working tree.
- Restrict network access (no allow-list, no namespace).
- Drop privileges, change uid/gid, or apply capabilities/seccomp.
- Hide or restrict other processes, the process table, or signals.
- Scrub or restrict environment variables (including any secrets).
- Enforce CPU/memory/time/output limits (beyond the tmpfs free-space guard).
- Provide any guarantee on non-Linux platforms (overlay is Linux-only and the
  prepare step fails fast elsewhere).

Treat `sandbox=overlay` as "don't dirty my checkout while this build/test runs",
not as "safe to run hostile code".

## Capability posture (current defaults)

| Dimension | Current behaviour |
|---|---|
| Filesystem (cwd subtree) | overlay copy-on-write, per-job upper |
| Filesystem (absolute paths) | inherit (no isolation) |
| Network | inherit (no isolation) |
| Process / PID / signals | inherit (no isolation) |
| Environment | inherit (resolved scope env) |
| Credentials / namespaces | inherit (no isolation) |
| Resource limits | only tmpfs free-space guard |

## Future hardening (not implemented)

Possible layers to add later, in rough order of value:

- Mount namespace + private `/tmp` and `$HOME` tmpfs per job.
- Network policy: deny-by-default with an allow-list (origin + path-prefix,
  reject ambiguous encoded separators, private-IP rules). See the URL allow-list
  design in `vercel-labs/just-bash` for a careful matcher.
- Landlock / seccomp filesystem and syscall restrictions.
- User namespace + privilege drop.
- cgroup CPU/memory limits and explicit `timeout` / output-size caps.
- A session-level shared overlay (`sandbox.scope=session`) whose writes persist
  across jobs within one session and are torn down on session GC.

These are intentionally out of scope for the current workspace-overlay
implementation and would be opt-in capabilities layered on top of it.

## Implementation references

- `crates/cue-daemon/src/sandbox.rs` — overlay prepare/cleanup, per-job upper
  root derivation, tmpfs free-space guard.
- `crates/cue-daemon/src/config.rs` — `[sandbox] default_upper_root` and
  `min_free_ratio`.
- `crates/cue-daemon/src/actor/process_mgr.rs` — wiring sandbox prepare into job
  spawn.
