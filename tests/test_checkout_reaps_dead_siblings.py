"""A private checkout whose worker died is reaped by a later checkout.

``materialize._execution_checkout`` removes its tree when the action ends,
but a worker killed mid-action (SIGKILL, OOM, a restart) never gets there,
and nothing else removed the tree: dl380g10 held ten such trees (1.4 GB),
seven of them nine days old, on 2026-10-15. Each new checkout now reaps the
dead siblings whose retention window has passed, and never a live one.

The dead worker here is a real process that materializes a checkout and is
SIGKILLed inside it. "Ageing" backdates every entry of the checkout root,
which is where a checkout keeps its owner lock and retention clock.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prismabuild import materialize  # noqa: E402
from test_pool import _materialization_item  # noqa: E402

MONTH_S = 30 * 24 * 3600
DAY_AND_AN_HOUR_S = 25 * 3600

HOLDER = """\
import json, sys
sys.path.insert(0, sys.argv[3])
from prismabuild import materialize
item = json.loads(sys.argv[1])
with materialize._execution_checkout(item, local_checkout_root=sys.argv[2]) as checkout:
    print(checkout, flush=True)
    sys.stdin.readline()
"""


def _holder(item: dict, base: Path) -> tuple[subprocess.Popen, Path]:
    """A worker that materializes a checkout and waits inside it."""

    process = subprocess.Popen(
        [sys.executable, "-c", HOLDER, json.dumps(item), str(base), str(ROOT / "src")],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    line = process.stdout.readline().strip()
    assert line, f"the holder never materialized (rc={process.poll()})"
    checkout = Path(line)
    assert (checkout / "payload.txt").read_text() == "sealed lifecycle bytes\n"
    return process, checkout.parent


def _checkout_once(item: dict, base: Path) -> Path:
    with materialize._execution_checkout(item, local_checkout_root=base) as checkout:
        assert (checkout / "payload.txt").is_file()
        return checkout.parent


def _age(base: Path, seconds: float) -> None:
    then = time.time() - seconds
    for path in base.iterdir():
        os.utime(path, (then, then), follow_symlinks=False)


def test_a_dead_workers_checkout_is_reaped_once_its_window_passes(tmp_path):
    item = _materialization_item(tmp_path)
    base = tmp_path / "checkouts"
    process, dead = _holder(item, base)
    process.kill()
    process.wait(timeout=60)
    assert (dead / "checkout" / "payload.txt").is_file()

    # The first checkout to find the owner gone starts the window; nothing
    # goes before it has passed from then.
    _checkout_once(item, base)
    assert dead.is_dir(), "a dead checkout was reaped inside its window"

    _age(base, DAY_AND_AN_HOUR_S)
    _checkout_once(item, base)
    assert not dead.exists(), "a dead worker's checkout outlived its window"
    assert sorted(path.name for path in base.iterdir()) == [], (
        "the reaped checkout left something behind")


def test_a_live_workers_checkout_is_kept_at_any_age(tmp_path):
    item = _materialization_item(tmp_path)
    base = tmp_path / "checkouts"
    process, live = _holder(item, base)
    try:
        _age(base, MONTH_S)
        for _ in range(2):
            _checkout_once(item, base)
            assert (live / "checkout" / "payload.txt").read_text() == (
                "sealed lifecycle bytes\n"), "a live checkout was reaped"
    finally:
        process.stdin.write("\n")
        process.stdin.flush()
        assert process.wait(timeout=60) == 0
    assert sorted(path.name for path in base.iterdir()) == [], (
        "a normal ending left its checkout or its lock behind")


def test_a_normal_checkout_leaves_nothing_in_the_root(tmp_path):
    item = _materialization_item(tmp_path)
    base = tmp_path / "checkouts"
    temporary = _checkout_once(item, base)
    assert not temporary.exists()
    assert sorted(path.name for path in base.iterdir()) == []


def test_unmarked_and_look_alike_entries_are_never_touched(tmp_path):
    item = _materialization_item(tmp_path)
    base = tmp_path / "checkouts"
    base.mkdir()
    outside = tmp_path / "outside"
    (outside / "checkout").mkdir(parents=True)
    (outside / "checkout" / "data").write_text("not PB's")
    legacy = base / f"{item['action_key'][:12]}.legacy01"
    (legacy / "checkout").mkdir(parents=True)
    (legacy / "checkout" / "payload.txt").write_text("pre-lock tree")
    (base / "0123456789ab.symlink0").symlink_to(outside)
    (base / "0123456789ab.symlink0.lock").symlink_to(outside / "checkout" / "data")
    (base / "notes.txt").write_text("an operator's file")
    (base / "0123456789AB.upper000").mkdir()
    _age(base, MONTH_S)
    before = sorted(path.relative_to(tmp_path).as_posix()
                    for path in tmp_path.rglob("*") if "materialization" not in str(path))

    _checkout_once(item, base)

    after = sorted(path.relative_to(tmp_path).as_posix()
                   for path in tmp_path.rglob("*") if "materialization" not in str(path))
    assert after == before
    assert (outside / "checkout" / "data").read_text() == "not PB's"
    assert (legacy / "checkout" / "payload.txt").read_text() == "pre-lock tree"
