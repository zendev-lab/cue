# cue-terminal

`cue-terminal` is cue-shell's concrete, Ghostty-backed foreground terminal
model. It owns terminal parsing, input encoding, scrollback, selection, and
Ratatui projection. It deliberately does not own a child process, PTY, IPC
connection, thread, or foreground controller lease.

The model is built on `libghostty-vt` 0.2.1 with its default Kitty graphics
feature disabled. `libghostty-vt` is not thread-safe, so a
`ForegroundTerminal` must remain on the TUI thread that created it.

## Attribution

The Ratatui projection and tracked-selection implementation were informed by
and adapted from:

- [`ratatui-ghostty`](https://codeberg.org/jint/ratatui-ghostty), MIT license.
- [`turborepo-ghostty`](https://github.com/vercel/turborepo/tree/main/crates/turborepo-ghostty),
  Copyright (c) 2026 Vercel, Inc., MIT license.

The required copyright and license notices are preserved in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
