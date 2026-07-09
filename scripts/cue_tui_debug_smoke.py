#!/usr/bin/env python3
"""Exercise cue-tui's first-party debug socket in a real PTY.

The smoke starts cue-tui with `--debug-socket`, drives it over the newline-JSON
control protocol, verifies a submitted job is visible in the rendered frame and
that stdout can be opened, then exits the TUI and removes the temporary socket.
"""

from __future__ import annotations

import fcntl
import json
import os
import pty
import re
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import threading
import time
from pathlib import Path
from typing import Any, TextIO

ROOT = Path(__file__).resolve().parents[1]
JOB_RE = re.compile(r"\[(J\d+)\]")


def main() -> int:
    binary = ensure_cue_tui_binary()
    with tempfile.TemporaryDirectory(prefix="cue-tui-debug-") as tmp:
        socket_path = Path(tmp) / "debug.sock"
        master_fd, slave_fd = pty.openpty()
        set_pty_size(slave_fd, rows=24, cols=80)
        proc = subprocess.Popen(  # noqa: S603 - local binary under test, argv is not shell-expanded.
            [str(binary), "--debug-socket", str(socket_path)],
            cwd=ROOT,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
        )
        drain_thread = threading.Thread(target=drain_pty, args=(master_fd,), daemon=True)
        drain_thread.start()
        os.close(slave_fd)
        try:
            wait_for_socket(socket_path, proc)
            wait_for_capture(socket_path, "Command Log", timeout_s=8)
            initial_state = request(socket_path, {"id": 1, "command": "state"})["ok"]["state"]

            subscriber = subscribe(socket_path)
            subscribed_frame = read_frame_until(subscriber, "Command Log", timeout_s=8)
            subscriber[1].close()
            subscriber[0].close()
            assert_contains(subscribed_frame["text"], "Command Log", "subscribed initial frame")

            command_text = f":run echo cue-debug-smoke-{proc.pid}"
            request(socket_path, {"id": 2, "command": "write-chars", "text": command_text})
            typed_state = wait_for_state_input(socket_path, command_text, timeout_s=4)
            assert_contains(typed_state["input"], command_text, "debug state input after write-chars")
            request(socket_path, {"id": 3, "command": "send-keys", "keys": ["enter"]})
            command_frame = wait_for_job_frame(socket_path, "cue-debug-smoke", timeout_s=8)
            job_id = parse_job_id(command_frame["text"])
            request(socket_path, {"id": 4, "command": "write-chars", "text": f":out {job_id}"})
            request(socket_path, {"id": 5, "command": "send-keys", "keys": ["enter"]})
            stdout_frame = wait_for_capture(socket_path, f"stdout {job_id}", timeout_s=8)
            assert_contains(stdout_frame["text"], f"cue-debug-smoke-{proc.pid}", "stdout display content")

            state = request(socket_path, {"id": 6, "command": "state"})["ok"]["state"]
            if state["connected"] is not True or state["mode"] != "JOB":
                raise AssertionError(f"unexpected debug state: {state!r}")
            if state["active_display_tab"] == initial_state["active_display_tab"]:
                raise AssertionError(f"active display tab did not change: before={initial_state!r} after={state!r}")
            request(socket_path, {"id": 7, "command": "send-keys", "keys": ["ctrl+d"]})
            proc.wait(timeout=8)
            socket_removed = cleanup_socket(socket_path)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "binary": str(binary),
                        "jobId": job_id,
                        "commandObserved": command_text,
                        "stdoutObserved": f"cue-debug-smoke-{proc.pid}",
                        "socketRemoved": socket_removed,
                        "processExitCode": proc.returncode,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        finally:
            try:
                os.close(master_fd)
            except OSError:
                pass
            if proc.poll() is None:
                request_best_effort(socket_path, {"id": 99, "command": "send-keys", "keys": ["ctrl+d"]})
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    proc.wait(timeout=3)
            cleanup_socket(socket_path)


def drain_pty(master_fd: int) -> None:
    while True:
        try:
            if not os.read(master_fd, 4096):
                return
        except OSError:
            return


def set_pty_size(fd: int, *, rows: int, cols: int) -> None:
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def ensure_cue_tui_binary() -> Path:
    binary = ROOT / "target" / "debug" / "cue-tui"
    subprocess.run(["cargo", "build", "-p", "cue-tui", "--bin", "cue-tui"], cwd=ROOT, check=True)  # noqa: S603,S607
    return binary


def wait_for_socket(socket_path: Path, proc: subprocess.Popen[Any], timeout_s: float = 12) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"cue-tui exited before debug socket appeared: {proc.returncode}")
        if socket_path.exists():
            return
        time.sleep(0.05)
    raise TimeoutError(f"debug socket did not appear: {socket_path}")


def request(socket_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(5)
        client.connect(str(socket_path))
        client.sendall(json.dumps(payload).encode() + b"\n")
        line = client.makefile("r", encoding="utf-8").readline()
    response = json.loads(line)
    if "err" in response:
        raise RuntimeError(response["err"])
    return response


def request_best_effort(socket_path: Path, payload: dict[str, Any]) -> None:
    try:
        if socket_path.exists():
            request(socket_path, payload)
    except Exception:
        pass


def subscribe(socket_path: Path) -> tuple[socket.socket, TextIO]:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(5)
    client.connect(str(socket_path))
    client.sendall(b'{"id":10,"command":"subscribe"}\n')
    file = client.makefile("r", encoding="utf-8")
    ack = json.loads(file.readline())
    if "err" in ack:
        raise RuntimeError(ack["err"])
    return client, file


def read_frame_until(
  subscriber: tuple[socket.socket, TextIO],
  needle: str,
  timeout_s: float,
) -> dict[str, Any]:
    client, file = subscriber
    client.settimeout(timeout_s)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        line = file.readline()
        if not line:
            raise RuntimeError("debug subscribe ended before frame")
        event = json.loads(line)
        if event.get("event") == "frame" and needle in event.get("text", ""):
            return event
    raise AssertionError(f"timed out waiting for subscribed frame containing {needle!r}")


def wait_for_state_input(socket_path: Path, needle: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        state = request(socket_path, {"id": 21, "command": "state"})["ok"]["state"]
        last = state
        if needle in state.get("input", ""):
            return state
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for state input {needle!r}; last state: {last!r}")


def wait_for_capture(socket_path: Path, needle: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last = ""
    while time.monotonic() < deadline:
        capture = request(socket_path, {"id": 20, "command": "capture"})["ok"]["capture"]
        last = capture["text"]
        if needle in last:
            return capture
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {needle!r}; last frame:\n{last}")


def wait_for_job_frame(socket_path: Path, needle: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last = ""
    while time.monotonic() < deadline:
        capture = request(socket_path, {"id": 22, "command": "capture"})["ok"]["capture"]
        last = capture["text"]
        if needle in last and JOB_RE.search(last):
            return capture
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for command record {needle!r}; last frame:\n{last}")


def parse_job_id(frame_text: str) -> str:
    match = JOB_RE.search(frame_text)
    if not match:
        raise AssertionError(f"could not find job id in frame:\n{frame_text}")
    return match.group(1)


def assert_contains(haystack: str, needle: str, label: str) -> None:
    if needle not in haystack:
        raise AssertionError(f"{label} did not contain {needle!r}:\n{haystack}")


def cleanup_socket(socket_path: Path) -> bool:
    if socket_path.exists():
        socket_path.unlink()
    return not socket_path.exists()


if __name__ == "__main__":
    sys.exit(main())
