"""Observe an actual old queue reader beside an introduced disjoint namespace."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import runpy
import sys


def operate(request, store, load):
    fixture = runpy.run_path(str(Path(__file__).with_name("compat_adapters.py")))["operate"]
    produced = fixture({"case": request["case"], "op": "write"}, store, load)
    own = json.loads(base64.b64decode(produced["raw"]))
    incoming = base64.b64decode(request["raw"])
    assert json.loads(incoming)["action_key"] == own["action_key"]
    pool = load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.item_path(pool.READY, own["action_key"]).write_bytes(incoming)
    before = queue.ready_items()
    assert len(before) == 1
    raw = (store / "introduced-ending.json").read_bytes()
    body = json.loads(raw)
    key = pool.PoolQueue.attempt_generation(body)
    path = queue.root / "publication-endings" / body["action_key"] / (key + ".json")
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    path.chmod(0o444)
    accessed = []
    def record(event, args):
        if event == "open" and isinstance(args[0], (str, bytes)) and Path(os.fsdecode(args[0])) == path:
            accessed.append(True)
    sys.addaudithook(record)
    request["_consumer_boundary"]()
    after = queue.ready_items()
    assert after == before and not accessed
    assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(raw).digest()
    return {"ready_keys": [row["action_key"] for row in after], "ending_codec_credit": False}
