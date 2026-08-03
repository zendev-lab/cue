# Third-party notices

cue-shell's release wheels statically include the Ghostty VT engine and code
from its verified native build closure. The `cue-terminal` Ratatui projection
also contains MIT-licensed adaptations. This notice covers the dependencies
introduced by that native terminal integration; it is not a complete SBOM for
the Rust workspace. This file and the referenced license texts are distributed
inside every wheel.

## Components

| Component | Pinned source | License |
|---|---|---|
| Ghostty / libghostty-vt | `a887df42c56f6de86c0fe6da9c4eeca37931e083` | MIT |
| libghostty-vt and libghostty-vt-sys Rust bindings | `0.2.1` / `46a9d2ac941ed600cf43c5e6299c8dfd1d3a1ef0` | MIT OR Apache-2.0; cue-shell redistributes under MIT |
| ratatui-ghostty adapted projection code | 0.2.0 | MIT |
| turborepo-ghostty adapted tracked-selection code | 2026 source snapshot | MIT |
| uucode | 0.2.0 | MIT; bundled Unicode data under Unicode-3.0; UTF-8 decoder portions under MIT |
| simdutf | 5.2.8 | MIT OR Apache-2.0; cue-shell redistributes under MIT |
| Highway | `66486a10623fa0d72fe91260f96c892e41aceb06` | Apache-2.0 and BSD-3-Clause |
| zlib | `1220fed0c74e1019b3ee29edae2051788b080cd96e90d56836eea857b0b966742efb` | Zlib |
| Zig compiler and UBSan runtimes | 0.15.2 | MIT |

The Apache-2.0, Highway BSD-3-Clause, Unicode-3.0, and Zlib license texts are
provided in the `licenses/` directory.

## MIT notices

- Copyright (c) 2024 Mitchell Hashimoto, Ghostty contributors
- Copyright (c) 2026 Uzair Aftab, Leah Amelia Chen
- Copyright (c) ratatui-ghostty contributors
- Copyright (c) 2026 Vercel, Inc.
- Copyright (c) 2026 Jacob Sandlund
- Copyright (c) 2008-2009 Bjoern Hoehrmann <bjoern@hoehrmann.de>
- Copyright 2021 The simdutf authors
- Copyright (c) Zig contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notices and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
