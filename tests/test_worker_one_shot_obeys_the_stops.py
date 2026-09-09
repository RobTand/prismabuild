"""A one-shot worker answers to the same two stops the loop does.

``tools/fleet/worker.py`` ships in every sealed generation
(``tools/fleet/publish_runtime.py`` ``FLEET_SCRIPTS``) and admits real work off
the live pool.  It read no maintenance gate, compared no generation, and left
``containment`` at its default, so its claim never reached the broker's
draining refusal either.  A drained host was therefore drained only for the
processes that agreed to be.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import pool

KEY = "c" * 64


def exercise(tmp_path, *, draining, loaded, live):
    """Run worker.main() in a child against a private queue and gate."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.publish(action_key=KEY, cas_root=str(tmp_path / "cas"),
                  checkout_root=str(tmp_path), worker_script="worker.py",
                  max_attempts=1)
    gate = tmp_path / "maintenance.json"
    if draining is not None:
        gate.write_text(json.dumps({
            "schema": "prismabuild.resource-maintenance.v1", "draining": draining}))
    for name, generation in (("loaded.json", loaded), ("live.json", live)):
        (tmp_path / name).write_text(json.dumps({"generation": generation}))

    script = tmp_path / "exercise.py"
    script.write_text(f'''
import json, sys
from pathlib import Path
sys.path.insert(0, {str(REPOSITORY / "src")!r})
sys.path.insert(0, {str(REPOSITORY / "tools" / "fleet")!r})
import worker_loop as wl
wl.MAINTENANCE_GATE = Path({str(gate)!r})
import worker
worker.SH = Path({str(tmp_path)!r})
worker.GENERATION_VERSION = Path({str(tmp_path / "loaded.json")!r})
worker.RUNTIME_VERSION = Path({str(tmp_path / "live.json")!r})
seen = {{}}
def serve_once(self, **kwargs):
    seen["containment"] = kwargs.get("containment")
    item = self.claim(tags=kwargs["tags"], has_gpu=kwargs["has_gpu"])
    seen["claimed"] = item is not None
    return None
worker.pool.PoolQueue.serve_once = serve_once
code = worker.main()
Path({str(tmp_path / "seen.json")!r}).write_text(json.dumps(seen))
raise SystemExit(code)
''')
    result = subprocess.run([sys.executable, str(script)], capture_output=True,
                            text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    seen = json.loads((tmp_path / "seen.json").read_text())
    return queue, seen, result.stdout


@pytest.mark.parametrize("draining,loaded,live,why", [
    (True, "gen-a", "gen-a", "resource broker draining"),
    (False, "gen-a", "gen-b", "runtime moved"),
    (None, "gen-a", "gen-b", "runtime moved"),
])
def test_a_stopped_host_is_stopped_for_the_one_shot_too(tmp_path, draining, loaded,
                                                        live, why):
    queue, seen, stdout = exercise(tmp_path, draining=draining, loaded=loaded,
                                  live=live)
    assert seen == {}, "serve_once ran while the host was stopped"
    assert queue.item_path(pool.READY, KEY).exists(), "the item was claimed anyway"
    assert why in json.loads(stdout)["declined"]


def test_an_admitting_host_serves_the_claim_in_front_of_the_broker(tmp_path):
    queue, seen, _ = exercise(tmp_path, draining=False, loaded="gen-a", live="gen-a")
    assert seen["claimed"] is True, "an open host must still serve"
    assert seen["containment"] is True, (
        "an uncontained claim never reaches the broker's draining refusal")


def test_an_unreadable_gate_stops_admission(tmp_path):
    """The loop fails closed on a corrupt gate; so must the one-shot."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.publish(action_key=KEY, cas_root=str(tmp_path / "cas"),
                  checkout_root=str(tmp_path), worker_script="worker.py",
                  max_attempts=1)
    gate = tmp_path / "maintenance.json"
    gate.write_text("{ this is not json")
    for name in ("loaded.json", "live.json"):
        (tmp_path / name).write_text(json.dumps({"generation": "gen-a"}))
    script = tmp_path / "exercise.py"
    script.write_text(f'''
import sys
from pathlib import Path
sys.path.insert(0, {str(REPOSITORY / "src")!r})
sys.path.insert(0, {str(REPOSITORY / "tools" / "fleet")!r})
import worker_loop as wl
wl.MAINTENANCE_GATE = Path({str(gate)!r})
import worker
worker.SH = Path({str(tmp_path)!r})
worker.GENERATION_VERSION = Path({str(tmp_path / "loaded.json")!r})
worker.RUNTIME_VERSION = Path({str(tmp_path / "live.json")!r})
def serve_once(self, **kwargs):
    raise AssertionError("served with an unreadable maintenance gate")
worker.pool.PoolQueue.serve_once = serve_once
raise SystemExit(worker.main())
''')
    result = subprocess.run([sys.executable, str(script)], capture_output=True,
                            text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert queue.item_path(pool.READY, KEY).exists()
