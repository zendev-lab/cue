"""Durable resource provider with one-shot lost responses and explicit failures."""

import json
import pathlib
import sys
import time

root = pathlib.Path(sys.argv[1])
request = json.load(sys.stdin)
with (root / "calls.jsonl").open("a") as log:
    log.write(json.dumps(request) + "\n")
key = request["daemon_id"] + "-" + request["request_id"]
ledger = root / (key + ".json")
method = request["method"]


def reply(value):
    print(json.dumps(value), flush=True)


def once(name):
    flag = root / name
    if flag.exists():
        flag.unlink()
        return True
    return False


if method == "probe":
    reply({"status": "snapshot", "data": {"worker": "1"}})
elif method == "lookup":
    reply(json.loads(ledger.read_text()) if ledger.exists() else {"status": "absent"})
elif method == "reserve":
    if (root / "reject").exists():
        reply({"status": "rejected", "reason": "capacity"})
    elif once("timeout"):
        time.sleep(30)
    else:
        if not ledger.exists():
            ledger.write_text(json.dumps({"status": "granted", "grant": {"id": key, "environment": {"CUDA_VISIBLE_DEVICES": "GPU-fixture"}, "devices": ["GPU-fixture"]}}))
        if not once("lose_reserve"):
            reply(json.loads(ledger.read_text()))
elif method == "release":
    if (root / "fail_release").exists():
        reply({"status": "unknown", "reason": "injected release failure"})
    else:
        ledger.write_text(json.dumps({"status": "released"}))
        if not once("lose_release"):
            reply({"status": "released"})
