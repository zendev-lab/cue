#!/usr/bin/env python3
"""Compute cue-shell's canonical digest for a read-only source tree."""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import struct
import tempfile
from pathlib import Path
from typing import Any, List, Optional, Tuple

FORMAT_PREFIX = b"cue-tree-digest-v1\0"
CHUNK_SIZE = 1024 * 1024

Entry = Tuple[bytes, bytes, Optional[bytes]]


def _append_blob(digest: Any, value: bytes) -> None:
    digest.update(struct.pack(">Q", len(value)))
    digest.update(value)


def _collect_entries(directory: bytes, prefix: bytes, entries: List[Entry]) -> None:
    with os.scandir(directory) as scan:
        children = list(scan)

    for entry in children:
        relative = entry.name if not prefix else prefix + b"/" + entry.name
        mode = entry.stat(follow_symlinks=False).st_mode
        if stat.S_ISDIR(mode):
            entries.append((relative, b"D", None))
            _collect_entries(entry.path, relative, entries)
        elif stat.S_ISREG(mode):
            entries.append((relative, b"F", entry.path))
        elif stat.S_ISLNK(mode):
            entries.append((relative, b"L", os.readlink(entry.path)))
        else:
            raise ValueError(f"unsupported filesystem node: {os.fsdecode(relative)}")


def tree_digest(root: Path) -> str:
    """Hash paths, file bytes, symlink targets, and executable bits."""
    root_bytes = os.fsencode(root.absolute())
    if not os.path.isdir(root_bytes):
        raise ValueError(f"not a directory: {root}")

    entries: List[Entry] = []
    _collect_entries(root_bytes, b"", entries)
    entries.sort(key=lambda item: item[0])

    digest = hashlib.sha256(FORMAT_PREFIX)
    for relative, kind, payload in entries:
        digest.update(kind)
        _append_blob(digest, relative)
        if kind == b"F":
            assert payload is not None
            _append_file(digest, relative, payload)
        elif kind == b"L":
            assert payload is not None
            _append_blob(digest, payload)
    return digest.hexdigest()


def _append_file(digest: Any, relative: bytes, path: bytes) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb", buffering=0) as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"file changed type: {os.fsdecode(relative)}")

        digest.update(b"1" if before.st_mode & 0o111 else b"0")
        digest.update(struct.pack(">Q", before.st_size))
        total = 0
        while chunk := stream.read(CHUNK_SIZE):
            total += len(chunk)
            digest.update(chunk)

        after = os.fstat(stream.fileno())
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if total != before.st_size or before_identity != after_identity:
            raise ValueError(f"file changed while hashing: {os.fsdecode(relative)}")


def _self_test() -> None:
    with tempfile.TemporaryDirectory(prefix="cue-tree-digest.") as temporary:
        root = Path(temporary)
        regular = root / "regular file"
        executable = root / "executable"
        empty = root / "empty"
        link = root / "link"
        regular.write_bytes(b"alpha")
        executable.write_bytes(b"#!/bin/sh\n")
        executable.chmod(0o755)
        empty.mkdir()
        link.symlink_to("regular file")
        baseline = tree_digest(root)

        regular.write_bytes(b"bravo")
        assert tree_digest(root) != baseline
        regular.write_bytes(b"alpha")
        assert tree_digest(root) == baseline

        executable.chmod(0o644)
        assert tree_digest(root) != baseline
        executable.chmod(0o755)
        assert tree_digest(root) == baseline

        link.unlink()
        link.symlink_to("executable")
        assert tree_digest(root) != baseline
        link.unlink()
        link.symlink_to("regular file")
        assert tree_digest(root) == baseline

        empty.rmdir()
        assert tree_digest(root) != baseline


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path)
    parser.add_argument("--self-test", action="store_true")
    arguments = parser.parse_args()
    if arguments.self_test:
        _self_test()
        return
    if arguments.root is None:
        parser.error("root is required unless --self-test is used")
    print(tree_digest(arguments.root))


if __name__ == "__main__":
    main()
