# cued Daemon Architecture Design

## 1. Overview

cued is a long-running background daemon that manages Jobs, Agents, Crons, and Scopes.
Clients (TUI, CLI, MCP) connect via Unix socket. cued owns the parser, scheduler,
process manager, and all persistent state.

## 2. Runtime Model

**Multi-threaded Tokio** (`tokio::runtime::Builder::new_multi_thread()`).

Rationale:
- Future-proofing for heavy Cron loads, multiple clients, and extension system
- Jobs and Agents are OS child processes (natural isolation from daemon)
- Competing tools: Zellij uses multi-threaded tokio for similar reasons
- `Send + Sync` overhead is acceptable given the Actor model (message passing, minimal shared state)

## 3. Actor Model

5 core Actors, each a `tokio::spawn`-ed task with its own `mpsc::Receiver<Msg>`.
Communication is exclusively via typed channel messages (no shared mutable state).

```
                    ┌─────────┐
     Client ───────▶│ Gateway │
     (Unix socket)  │         │◀──── EventBus (events to push)
                    └────┬────┘
                         │ Eval result / structured request
                         ▼
                    ┌───────────┐
                    │ Scheduler │
                    │           │◀──── CronTimer triggers
                    └─────┬─────┘
                          │ Spawn / Kill / Fg commands
                          ▼
                    ┌──────────────┐
                    │ ProcessMgr   │
                    │              │───▶ EventBus (state changes, output)
                    └──────────────┘
                          │ OS child processes (fork+exec+pty)
                          ▼
                    ┌──────────────┐
                    │ ScopeStore   │───▶ EventBus (scope events)
                    │ (SQLite)     │
                    └──────────────┘
```

### 3.1 Gateway

Responsibilities:
- Accept Unix socket connections, spawn per-client handler task
- Parse user input: Tokenizer → Parser → Resolver (full 3-layer pipeline)
- Route resolved requests to Scheduler
- Manage per-client subscriptions (channel model)
- Forward events from EventBus to subscribed clients
- Handle :fg proxy mode (FgInput/FgOutput forwarding)
- Serve Complete / Highlight editor requests

State: per-client subscription sets, per-client fg attachment status

### 3.2 Scheduler

Responsibilities:
- Assign Job/Agent/Cron IDs (monotonically increasing per type: J1, J2, ...)
- Manage Chain AST execution:
  - Track chain dependency graph
  - When a Job finishes, advance chain (serial → trigger next; parallel → check all)
  - Handle ChainAborted cascading (retry success → restart downstream)
- Manage Cron timers:
  - Maintain timer heap (next trigger time per active Cron)
  - On trigger: create RunJob request → ProcessManager
  - `in` type: auto-remove after single trigger
- Queue pending Jobs when resource limits are hit (future)
- Forward spawn/kill commands to ProcessManager

State: active chains (AST + execution state), active crons, ID counters

### 3.3 ProcessManager

Responsibilities:
- Spawn OS child processes (fork + exec + pty allocation)
- Set up pty pairs (master/slave) for interactive processes
- Collect stdout/stderr from pty master fd
  - Write to in-memory ring buffer (for real-time streaming)
  - Write to output log file (for persistence)
- Send OutputChunk events to EventBus
- Handle SIGCHLD / waitpid for process exit detection
- Report state changes to EventBus (Running → Done/Failed/Killed)
- Kill processes on request (SIGTERM → timeout → SIGKILL)
- Manage :fg pty proxy (forward input from Gateway, output to Gateway)
- Apply Scope (env vars + cwd) when spawning processes
- Capture end_scope (env snapshot after process exits, if applicable)

State: running process table (pid, pty fd, ring buffer, job/agent id mapping)

### 3.4 ScopeStore

Responsibilities:
- Create new Scope snapshots (blake3 hash of env + cwd + ...)
- Fork scopes (create new snapshot with parent link)
- Delta storage: parent_hash + EnvDelta (set/unset/cwd changes)
- Reconstruct full Scope by walking parent chain
- Manage HEAD pointer (default scope)
- Persist to SQLite (scopes table)
- Respond to scope queries from Scheduler/Gateway

State: in-memory cache of recently used scopes, HEAD pointer

### 3.5 EventBus

Responsibilities:
- Receive events from all Actors (ProcessMgr, Scheduler, ScopeStore)
- Fan-out events to Gateway based on channel subscriptions
- Buffer recent events for late-joining clients (optional, configurable)

Implementation: `tokio::sync::broadcast::channel` or custom fan-out with per-subscriber `mpsc`.
Prefer custom fan-out (broadcast drops slow consumers; mpsc with bounded buffer + backpressure is safer).

State: subscriber registry (Gateway handles per-client routing)

## 4. Storage Architecture (Three-Layer)

### Layer 1: In-Memory (Real-time)

- **Output ring buffer**: per-Job/Agent, fixed size (default 1 MiB), circular overwrite
  - Source of truth for OutputChunk events pushed to clients
  - Also serves `:out J1` tail queries for recent output
- **Active state**: running Jobs, Agents, Chains, Crons (in respective Actors)

### Layer 2: File System (Output Persistence)

- **Output log files**: `$XDG_DATA_HOME/cue-shell/output/J{id}.log`
  - Append-only write from ProcessManager as chunks arrive
  - Full historical output for `:out J1 --full` or :log queries
  - Auto-cleanup: configurable retention (by age or total size)
  - One file per Job/Agent, plain text (binary-safe with raw bytes)

### Layer 3: SQLite (Metadata Persistence)

- **Database**: `$XDG_DATA_HOME/cue-shell/cued.db`
- **WAL mode** for concurrent reads during writes
- Tables:

```sql
-- Scope snapshots (content-addressed)
CREATE TABLE scopes (
    hash        TEXT PRIMARY KEY,   -- blake3 hex
    parent_hash TEXT REFERENCES scopes(hash),
    delta       TEXT NOT NULL,      -- JSON: {"set": {...}, "unset": [...], "cwd": "..."}
    label       TEXT,               -- human label (S0, S1, ...)
    created_at  TEXT NOT NULL       -- ISO 8601
);

-- HEAD pointer
CREATE TABLE scope_head (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    hash TEXT NOT NULL REFERENCES scopes(hash)
);

-- Cron registry + history (restored on daemon restart)
CREATE TABLE crons (
    id          TEXT PRIMARY KEY,   -- C1, C2, ...
    schedule    TEXT NOT NULL,
    command     TEXT NOT NULL,      -- raw command / chain text
    enabled     INTEGER NOT NULL DEFAULT 1, -- legacy mirror of runnable state
    scope_hash  BLOB,
    status      TEXT,               -- scheduled / paused / completed / expired
    created_at  TEXT NOT NULL
);

-- Job history (completed jobs, for :log and analytics)
CREATE TABLE jobs_history (
    id           TEXT PRIMARY KEY,  -- J1, J2, ...
    pipeline     TEXT NOT NULL,
    state        TEXT NOT NULL,     -- Done, Failed, Killed, Cancelled
    cancel_reason TEXT,             -- User, ChainAborted, Timeout (if Cancelled)
    start_scope  TEXT REFERENCES scopes(hash),
    end_scope    TEXT REFERENCES scopes(hash),
    started_at   TEXT,
    finished_at  TEXT NOT NULL,
    exit_code    INTEGER
);

-- Agent history
CREATE TABLE agents_history (
    id          TEXT PRIMARY KEY,  -- A1, A2, ...
    kind        TEXT NOT NULL,     -- legacy compatibility mirror of backend
    backend     TEXT,              -- configured ACP backend/profile name
    role        TEXT NOT NULL,     -- Planner, Executor
    status      TEXT NOT NULL,
    session_id  TEXT,              -- ACP session id for resume/session-load
    model       TEXT,              -- optional model override
    scope_hash  BLOB,              -- scope snapshot used to relaunch the runtime
    transcript  TEXT NOT NULL,     -- persisted session transcript for reconnect/restart hydration
    last_role   TEXT,              -- last appended transcript role, used to continue chunk formatting
    created_at  TEXT NOT NULL,
    finished_at TEXT
);

-- Config overrides (mode param defaults from config.toml, cached)
CREATE TABLE config_cache (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

## 5. Data Directories (XDG)

```
$XDG_RUNTIME_DIR/cue-shell/
  cued.sock                    # Unix domain socket (process lifetime)
  cued.pid                     # PID file for auto-detection

$XDG_DATA_HOME/cue-shell/     # default: ~/.local/share/cue-shell/
  cued.db                      # SQLite database
  output/                      # Job output log files
    J1.log
    J2.log
    A1.log

$XDG_STATE_HOME/cue-shell/    # default: ~/.local/state/cue-shell/
  cued.log                     # daemon log (tracing-appender, rotated)

$XDG_CONFIG_HOME/cue-shell/   # default: ~/.config/cue-shell/
  config.toml                  # user configuration
```

macOS fallback: `$XDG_*` vars respected if set, otherwise:
- `$XDG_DATA_HOME` → `~/.local/share`
- `$XDG_CONFIG_HOME` → `~/.config`
- `$XDG_STATE_HOME` → `~/.local/state`
- `$XDG_RUNTIME_DIR` → `$TMPDIR` or `/tmp`

## 6. Startup Sequence

```
1. Parse CLI args (bpaf)
2. Check PID file → if running, print "cued already running (pid N)" and exit
3. Daemonize (fork, setsid, close stdin/stdout/stderr) — or foreground mode with --fg
4. Write PID file
5. Initialize tracing (file appender + rotation)
6. Open/create SQLite database (WAL mode, run migrations)
7. Create data directories (output/, etc.)
8. Restore state from SQLite:
   a. Load Scope HEAD
   b. Load active Crons → register timers in Scheduler
9. Spawn Actor tasks:
   a. EventBus
   b. ScopeStore (with SQLite handle)
   c. ProcessManager
   d. Scheduler (with Cron timers)
   e. Gateway (bind Unix socket)
10. Emit DaemonReady event
11. Enter tokio runtime main loop
```

## 7. Shutdown Sequence (Drain Mode)

Triggered by: `cued stop`, SIGTERM, SIGINT, or `:shutdown` command.

```
1. Enter DRAIN mode:
   a. Stop all Cron timers (no new triggers)
   b. Reject new Eval requests (respond with Err { code: "SHUTTING_DOWN" })
   c. Push ShuttingDown event to all connected clients

2. Wait for running Jobs/Agents to complete naturally
   - Monitor ProcessManager's active process count
   - No timeout yet — let them finish

3. Safety net timeout (default: 5 min, configurable `shutdown_max_wait`):
   a. Send SIGTERM to all remaining processes
   b. Wait 10 seconds
   c. Send SIGKILL to survivors

4. Persist final state:
   a. Flush Cron definitions to SQLite (already persistent, but ensure consistency)
   b. Flush Scope HEAD to SQLite
   c. Write jobs_history for any interrupted jobs (state = Killed)

5. Close Unix socket
6. Remove PID file
7. Exit(0)
```

## 8. Auto-Start (Client-Triggered)

When TUI/CLI tries to connect and finds no socket:

```
1. TUI: connect($SOCKET_PATH) → ENOENT / ECONNREFUSED
2. TUI: fork+exec `cued start --auto`
3. TUI: retry connect with exponential backoff (100ms, 200ms, 400ms, ... max 5s)
4. cued: emit DaemonReady → TUI connects successfully
5. If all retries fail: TUI shows "Failed to start cued daemon" error
```

`--auto` flag: cued logs "auto-started by client" for diagnostics.

## 9. Observability

- **tracing** crate with `tracing-subscriber` (fmt layer + file appender)
- Log levels via `RUST_LOG` env var or `config.toml`
- Default: `cued=info,cue_core=info`
- Debug mode: `cued=debug,cue_core=debug` (verbose Actor message tracing)
- Log rotation: `tracing-appender` daily rotation, keep last 7 files
- Future: `tracing-opentelemetry` for distributed tracing (extension system)

## 10. Future: Extension System

Design space for future extensions (not v1):
- **WASM plugins** (like Zellij) — sandboxed, safe, cross-platform
- Extensions get their own Actor with a constrained message interface
- Resource limits via WASM fuel metering
- Extension manifest in config.toml
- This is why we chose multi-threaded tokio — extensions may be CPU-intensive
