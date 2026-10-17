"""A real funded batch must not authorize an unknown materialization history.

The mixed history is deliberate disk corruption, installed only after real
prewrite, commitment, funding and (for the successor) stage/retire/refill.
No fabricated funding or immutable batch record supplies the authority.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import socket
import sys

import pytest

from test_produced_output_restage import (
    TIER, _World, _descriptors, _install, _prewrite, _staged_world,
    _isolated_synthetic_launch_context,
)


@pytest.mark.parametrize("successor", [False, True], ids=["first", "successor"])
@pytest.mark.parametrize("corrupt", [False, True], ids=["valid", "mixed-history"])
def test_funded_mover_authority_requires_known_history(tmp_path, successor, corrupt):
    if successor:
        world, _ = _staged_world(tmp_path)
        published = world.ensure("b1")
        assert published.get("ok") is True, published
    else:
        world = _World(tmp_path)
        descriptors = _descriptors(world.template, world.inst, "p1", b"history bytes")
        _prewrite(world.q, world.inst, world.template, "b1", descriptors)
        published = world.first_publish("b1", descriptors)
    mover = str(published["mover_key"])
    funded = world.q.funding.read_output(mover, TIER)
    assert funded is not None
    assert world.q.funding._output_batch_authority(funded) is True
    if corrupt:
        rows = world.entry("b1").get("materializations", [])
        _install(world, [None, *rows], batch_retired=successor)
    assert world.q.funding._output_batch_authority(funded) is (not corrupt)


def test_the_admitted_history_control_reports_its_actual_interpreter(capsys):
    with capsys.disabled():
        executable = Path("/proc/self/exe")
        raw = executable.read_bytes()
        print("c13-interpreter: " + json.dumps({
            "version": sys.version, "version_info": list(sys.version_info[:3]),
            "executable": sys.executable, "resolved": str(Path(sys.executable).resolve()),
            "mapped_executable": os.readlink(executable), "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(), "host": socket.gethostname(),
            "cpus": sorted(os.sched_getaffinity(0)), "tmpdir": os.environ.get("TMPDIR"),
        }, sort_keys=True), flush=True)
