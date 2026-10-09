"""A matching adoption restarts no loop that carries the shape (#1664).

The supervisor adopts the new generation's roster at re-exec. The
shape pass compares every live loop's own argv against the declared
args and stops only the idle loops that carry another shape. A loop
that already runs the declared args -- including a flag the defect
dropped, such as ``--gang-admission`` -- is never stopped for the
adoption, and the tick spawns nothing to replace it.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import supervise  # noqa: E402

HOST = "sparky"
SHAPE = ["--class", "gb10", "--gpu", "--all-cores", "--gang-admission"]


def _private_pool(tmp_path, monkeypatch) -> None:
    config = tmp_path / "fleet_boxes.json"
    config.write_text(json.dumps(
        {"boxes": {HOST: {"loops": 2, "args": SHAPE}}}))
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(supervise, "CLAIM", tmp_path / "supervisor.claim")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    monkeypatch.setattr(sys, "argv", ["supervise", "--once"])
    monkeypatch.setattr(supervise.time, "sleep", lambda _s: None)
    monkeypatch.setattr(supervise, "_live_loops", lambda *_a, **_k: [11, 22])
    monkeypatch.setattr(supervise, "_is_fleet_loop", lambda *_a, **_k: True)
    monkeypatch.setattr(supervise, "loop_args_of", lambda _pid: list(SHAPE))
    monkeypatch.setattr(supervise, "_is_idle", lambda _pid, *_a: True)


def test_a_matching_adoption_restarts_no_loop(tmp_path, monkeypatch) -> None:
    _private_pool(tmp_path, monkeypatch)
    killed: list[int] = []
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, _sig: killed.append(pid))
    spawned: list[list[str]] = []
    monkeypatch.setattr(supervise, "_spawn",
                        lambda args, index: spawned.append(list(args)) or 900)

    assert supervise.main() == 0
    assert killed == [], "a loop on the declared shape was stopped"
    assert spawned == [], "a matching adoption spawned a replacement loop"
