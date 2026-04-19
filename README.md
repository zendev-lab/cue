# cue-shell

An async process runtime with TUI for human-agent collaboration.

> ⚠️ **Prototype stage** — core JOB / AGENT / CRON flows are implemented, including
> multi-turn agents and real `:fg` PTY attach; remaining work is mainly mode-specific UI polish.

## Overview

cue-shell (`cue`) is a terminal-native runtime for orchestrating async processes, designed for seamless human-agent collaboration. It is **not** a traditional shell — it's a structured environment where jobs, sessions, and scopes are first-class primitives.

### Key Features

- **Three-layer architecture**: Execution Backend (`cued` daemon) → Session Runtime (core) → Frontend (TUI/MCP/API)
- **Three interaction modes**: JOB ⚡ · AGENT 🤖 · CRON ⏰ — switch with `Shift+Tab`
- **`:` prefix commands**: Vim-style builtin access (`:run`, `:kill`, `:jobs`, `:ask`, `:cron`, ...)
- **Planner/Executor model**: Structured AI collaboration with `:ask`, `:spawn`, `:agents`
- **Multi-turn agents**: follow-up via `:send A<n> ...`, abort current turn via `:cancel A<n>`, terminate session via `:kill A<n>`
- **Foreground PTY attach**: `:fg J<n>` proxies a real terminal session with input, paste, and resize support
- **Display tabs with clean semantics**: `:out J<n>` snapshots stdout, `:tail J<n>` follows live stdout, `:err J<n>` opens stderr
- **Scope persistence**: Environment snapshots with delta storage and lifecycle management
- **Chain syntax**: `->` serial · `~>` ignore-failure · `||` parallel · `||?` any-success
- **Daemon durability**: persisted HEAD scope, job history, cron definitions, auto-reconnect TUI

## Architecture

```
┌─────────────────────────────────────────┐
│  L3 Frontend: TUI / MCP / REST API      │
├─────────────────────────────────────────┤
│  L2 Session Runtime (cue-core)          │
│  Job · Session · Scope · Chain          │
├─────────────────────────────────────────┤
│  L1 Execution Backend (cued daemon)     │
│  Unix socket · SQLite · Process mgmt    │
└─────────────────────────────────────────┘
```

## Workspace Structure

```
crates/
├── cue-core/   — Core types and logic: Job, Session, Scope, Chain
├── cued/       — Background daemon: Unix socket server, job orchestration
├── cue-tui/    — TUI frontend: mode switching, command input, job display
└── cue-cli/    — CLI entry point: command parsing, mode dispatch
```

## Development

```bash
# Prerequisites: Rust 1.90+, just

# Build
cargo build

# Start daemon in foreground
cued -f

# Start TUI (auto-connect / auto-reconnect)
cargo run -p cue-cli -- tui

# Run checks
just check

# Run tests
just test

# Full CI locally
just ci

# Install pre-commit hooks
just pre-commit-install
```

## Design Documents

See [`docs/design/`](docs/design/) for the full design documentation:

- **DESIGN.md** — Architecture, core concepts, three-layer model
- **commands-and-modes.md** — Command reference, mode system, `:cron` syntax

## Client + server config

cue-shell now prefers a split config layout in the platform config dir:

- `client.toml` — client-side transport/profile selection used by `cue`
- `server.toml` — daemon-side agent backend defaults used by `cued`

During migration, cue-shell still falls back to the legacy combined
`config.toml`. If you keep using that file for now, put client transport under
`[transport]` and server agent backend settings under `[agent]`.

### Client transport config

`cue` defaults to a local Unix socket profile, so local users do not need any
config for the current flow. To make the split explicit:

```toml
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"
# socket = "/custom/path/to/cued.sock"

[transport.profiles.remote-dev]
transport = "ssh"
destination = "user@example.com"
gateway_command = "cued gateway --stdio"
start_command = "cued start"
```

Phase 1 uses the system OpenSSH client and runs the configured gateway command
over SSH, so the client speaks the same IPC through `cued gateway --stdio`.
Remote daemon startup still stays explicit: `cue` will **not** run
`start_command` for you.

Typical remote flow:

```bash
# Step 1: start the remote daemon explicitly
ssh user@example.com "cued start"

# Step 2: connect with the SSH transport profile
cue
```

If the remote daemon is not running (or its socket is missing), `cue` fails
with a message that includes the profile's explicit `start_command`.

### Agent backend config

`cued` now prefers `server.toml` (for example
`$XDG_CONFIG_HOME/cue-shell/server.toml` on Linux/macOS with XDG set).

Current AGENT runtime is **ACP-only**. By default, cue-shell starts GitHub
Copilot CLI as an ACP server via `copilot --acp --stdio`. Override it in
`server.toml` if you want a different ACP backend:

```toml
[agent]
default_backend = "copilot"

[agent.backends.copilot]
command = "copilot"
args = ["--acp", "--stdio"]
# model = "your-model"
```

Per-command params like `:ask(model=gpt-5)` or `:spawn(agent=copilot)` override these
defaults. `:ask` / `:spawn` start an ACP session, `:send A<n> ...` continues it
via another `session/prompt`, `:cancel A<n>` maps to `session/cancel`, and
`session=<id>` can be used to load an existing ACP session when the backend
advertises `loadSession`.

## Project Status

| Component | Status |
|-----------|--------|
| Design docs | ✅ Active |
| Cargo workspace | ✅ Scaffolded |
| CI/CD | ✅ Configured |
| cue-core | ✅ Core types / IPC / parser in place |
| cued daemon | 🚧 Functional prototype |
| cue-tui | 🚧 Functional prototype |
| cue-cli | 🚧 Functional prototype |

## License

Licensed under the [MIT License](LICENSE).
