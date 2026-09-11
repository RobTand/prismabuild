"""#458: a reboot must not silently reopen a held maintenance drain.

These are regression reproductions only.  Both model a loss of a private
``/run`` stand-in; neither reads or changes the host's actual volatile or
persistent state.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
from pathlib import Path


TESTS = Path(__file__).resolve().parent


def _load_test_module(name: str, path: Path):
    """Reuse the existing private test fixtures without introducing a new API."""

    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_worker_loop_keeps_maintenance_requested_after_private_run_loss(tmp_path):
    """A gate held before reboot remains a stop after its volatile copy is lost."""

    proofs = _load_test_module(
        "worker_loop_proves_it_parked_for_458",
        TESTS / "test_worker_loop_proves_it_parked.py",
    )
    gate = tmp_path / "private-run" / "maintenance.json"
    gate.parent.mkdir()
    gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))

    assert proofs._worker_loop(gate).maintenance_requested() is True

    # This is the private test equivalent of /run being cleared at boot.
    gate.unlink()
    rebooted_loop = proofs._worker_loop(gate)

    assert rebooted_loop.maintenance_requested() is True


def test_named_maintenance_hold_blocks_new_scope_after_private_run_loss(tmp_path):
    """A named hold survives loss of the broker's private volatile files."""

    fixtures = _load_test_module(
        "resource_broker_fixtures_for_458",
        TESTS / "test_resource_broker.py",
    )
    broker_module = fixtures.module()
    private_run = tmp_path / "private-run"
    jobs = private_run / "jobs"
    durable = tmp_path / "private-var" / "maintenance.json"
    holder = "test-458-reboot-holder"
    private_run.mkdir()
    (private_run / "maintenance.json").write_text(json.dumps({
        "schema": "prismabuild.resource-maintenance.v1", "draining": False,
        "changed_unix": 1.0, "reason": "pre-durable test initialization"}))
    (private_run / "maintenance.json").chmod(0o644)
    authority = broker_module.Authority(
        jobs,
        os.getuid(),
        fixtures.Backend(),
        max_memory_bytes=1024 ** 3,
        maintenance_state=durable,
    )

    held = authority.handle(
        0,
        os.getpid(),
        {"op": "maintenance_begin", "reason": "#458 reboot reproduction", "owner": holder},
    )
    assert held["draining"] is True
    assert held["maintenance_owner"] == holder

    # Delete exactly the two private volatile broker artifacts, then use a
    # fresh kernel facade as a rebooted broker would.  No host path is touched.
    shutil.rmtree(jobs)
    authority.maintenance_path.unlink()
    assert list(private_run.iterdir()) == []
    rebooted = broker_module.Authority(
        jobs,
        os.getuid(),
        fixtures.Backend(),
        max_memory_bytes=1024 ** 3,
        maintenance_state=durable,
    )

    response = rebooted.handle(
        os.getuid(),
        os.getpid(),
        {
            "op": "create",
            "action_key": "4" * 64,
            "nonce": "5" * 32,
            "memory_max_bytes": 64 * 1024 ** 2,
        },
    )

    assert response["ok"] is False
    assert response["maintenance"] is True
    assert response["retryable"] is True
