# TUI debug control

External processes (test harnesses, AI agents) can drive and observe a running `cue-tui` without owning its terminal. The design mirrors the control surface that proved useful in Spark's [zellij harness](https://github.com/zrr1999/spark/blob/main/docs/spark-zellij-harness.md): launch the TUI in a real terminal, inject input, capture rendered pane text, inspect state, and subscribe to frame changes.

Unlike `cued` IPC (length-prefixed JSON to the daemon), debug control lives in the **TUI process** because rendering and input focus are owned there.

## Opt-in activation

Debug control is **off by default**. Enable it with either:

- CLI flag: `cue-tui --debug-socket <path>`
- Environment: `CUE_TUI_DEBUG_SOCKET=<path>`

When set, `cue-tui` binds a Unix domain socket (mode `0600`) alongside its normal event loop. No behavior change when the flag/env is unset.

## Transport and framing

- **Transport**: Unix domain socket
- **Framing**: newline-delimited JSON (one request or response/event per line)
- **Wire types**: `cue_core::tui_debug` (request/response envelopes)

### Request envelope

```json
{"id":1,"command":"capture","styled":false}
{"id":2,"command":"send-keys","keys":["enter","ctrl+c"]}
{"id":3,"command":"write-chars","text":"hello"}
{"id":4,"command":"state"}
{"id":5,"command":"subscribe","styled":false}
```

### Response envelope

Success:

```json
{"id":1,"ok":{"capture":{"text":"...","width":80,"height":24}}}
{"id":2,"ok":{"ack":{}}}
{"id":4,"ok":{"state":{"mode":"JOB","focus":"input","input":":run ","sidebar_visible":true,"connected":true,"job_count":3,"cron_count":1,"active_display_tab":0,"display_tab_labels":["stdout"],"fg_active":false,"should_quit":false,"terminal_width":120,"terminal_height":40}}}
```

Error:

```json
{"id":1,"err":{"code":"INVALID_REQUEST","message":"unknown key `foo`"}}
```

Subscribe streams pushed frame events after an initial `{"id":N,"ok":{"ack":{}}}` acknowledgement:

```json
{"event":"frame","text":"...","width":80,"height":24}
```

Identical consecutive frames are debounced (not re-sent).

## Commands

| Command | Purpose |
| --- | --- |
| `capture` | Return the last rendered frame as plain text; pass `"styled":true` for ANSI |
| `send-keys` | Inject named key events (`enter`, `esc`, `ctrl+c`, `shift+tab`, arrows, single chars) |
| `write-chars` | Inject a string as per-character key events |
| `state` | Return a JSON summary of app state for assertions |
| `subscribe` | Stream `frame` events whenever the rendered buffer changes |

Malformed requests return structured errors and never crash the TUI.

## Thin client (`cue-tui debug`)

The `cue-tui` binary includes a thin client so harnesses do not need a separate tool:

```bash
# Terminal 1 — start TUI with debug socket
cue-tui --debug-socket /tmp/cue-tui-debug.sock

# Terminal 2 — drive/observe
cue-tui debug capture --socket /tmp/cue-tui-debug.sock
cue-tui debug capture --socket /tmp/cue-tui-debug.sock --styled
cue-tui debug send-keys --socket /tmp/cue-tui-debug.sock enter ctrl+c
cue-tui debug write-chars --socket /tmp/cue-tui-debug.sock ":run echo hello"
cue-tui debug state --socket /tmp/cue-tui-debug.sock
cue-tui debug subscribe --socket /tmp/cue-tui-debug.sock
```

`CUE_TUI_DEBUG_SOCKET` can replace `--socket` on debug client commands.

## Automated smoke

Use the repository smoke when changing TUI rendering, key handling, debug-control transport, or cleanup behavior:

```bash
just tui-debug-smoke
```

The smoke launches `cue-tui --debug-socket` in a real PTY, drives the socket directly, subscribes to frame changes, submits `:run echo ...`, opens the job stdout tab, verifies the rendered command record and stdout content, exits with `ctrl+d`, and removes the temporary socket. This is the primary product-correctness path for cue-tui automation; zellij is only needed when validating an external terminal/session-manager integration.

## Rationale (zellij-inspired)

Spark's native TUI validation uses zellij as an outer session manager: an external process launches a pane, sends keys, captures scrollback, and subscribes to pane updates. cue-shell needs the same capability without requiring zellij:

1. **Harness launch** — run `cue-tui` in a real terminal with `--debug-socket`
2. **Inject input** — `send-keys` / `write-chars` feed the TUI event loop
3. **Capture rendered text** — `capture` serializes the last ratatui buffer
4. **Inspect state** — `state` exposes mode, focus, input line, connection status, counts
5. **Subscribe** — `subscribe` streams frame snapshots for async assertions
6. **Cleanup** — debug server shuts down with the TUI; socket is removed on restart

Keeping this surface in `cue-tui` (not `cued`) ensures captures reflect what the user actually sees and keys land in the correct focus context.

## Safety

- Socket path is created with `0600` permissions on Unix
- Debug server task stops when the TUI exits
- Malformed JSON or unknown commands return `INVALID_REQUEST` / `UNAVAILABLE` errors

## Implementation map

| Area | Location |
| --- | --- |
| Wire types | `crates/cue-core/src/tui_debug.rs` |
| Buffer serialization, key parsing | `crates/cue-tui/src/tui_debug/buffer.rs`, `keys.rs` |
| Socket server | `crates/cue-tui/src/tui_debug/server.rs` |
| CLI client | `crates/cue-tui/src/tui_debug/client.rs`, `crates/cue-tui/src/cli.rs` |
| TUI integration | `crates/cue-tui/src/lib.rs` (snapshot after draw, inject via event channel) |
