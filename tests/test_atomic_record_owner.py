"""One owner for rename-atomic JSON records (PB #1330).

``materialize._write_json_atomic`` is the owner the pool queue writers
already use. The hand-rolled remainder re-spell tmp+replace with their
own durability choices; the owner parametrizes exactly the axes that
legitimately differ (fsync or not, mode, temp naming, dir-fsync,
sorted-text vs canonical bytes, make-parent) so every migrated site
keeps its semantics bit for bit -- never silently upgraded or
downgraded. These tests pin the parameters, not the call sites.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from prismabuild import materialize


def _read(path: Path) -> bytes:
    return path.read_bytes()


def test_canonical_bytes_are_the_default():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "r.json"
        materialize._write_json_atomic(path, {"b": 1, "a": [1, 2]})
        assert _read(path) == b'{"a":[1,2],"b":1}'


def test_sorted_text_bytes_match_the_hand_rolled_writers():
    """``resource_scope._atomic_json`` and friends wrote exactly this."""

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "r.json"
        materialize._write_json_atomic(path, {"b": 1, "a": [1, 2]},
                                       text="sorted_lf")
        assert _read(path) == (json.dumps({"b": 1, "a": [1, 2]},
                                          sort_keys=True) + "\n").encode()


def test_canonical_lf_bytes_match_the_lane_history():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "r.json"
        materialize._write_json_atomic(path, {"b": 1},
                                       trailing_newline=True)
        assert _read(path) == b'{"b":1}\n'


def test_no_fsync_still_lands_and_leaves_no_litter():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "r.json"
        materialize._write_json_atomic(path, {"a": 1}, text="sorted_lf",
                                       fsync=False, tmp="pid")
        assert _read(path) == b'{"a": 1}\n'
        assert [p.name for p in Path(tmp).iterdir()] == ["r.json"]


def test_temp_styles_land_and_clean_up():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        for style in ("pid_uuid", "pid"):
            path = Path(tmp) / f"{style}.json"
            materialize._write_json_atomic(path, {"a": 1}, tmp=style)
            assert _read(path) == b'{"a":1}'
        assert sorted(p.name for p in Path(tmp).iterdir()) == [
            "pid.json", "pid_uuid.json"]


def test_no_mkdir_leaves_the_missing_directory_missing():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "no-such-dir" / "r.json"
        try:
            materialize._write_json_atomic(missing, {"a": 1},
                                           make_parent=False)
        except FileNotFoundError:
            pass
        else:                                                  # pragma: no cover
            raise AssertionError("make_parent=False created a directory")


def test_pid_temp_truncates_a_stale_temp_left_by_a_dead_writer():
    """A SIGKILLed writer's stale pid temp must not wedge the path (#1331).

    The migrated writers used ``write_text``/``open(\"w\")``, which
    truncate: with ``O_EXCL`` a stale ``.<name>.<pid>.tmp`` (and PIDs get
    reused, especially in containers) makes every later write raise
    ``FileExistsError`` -- silently swallowed by the status sidecars,
    and progress records feed stall detection, so a healthy row could
    be withdrawn as stalled.  ``tmp=\"pid\"`` keeps ``O_TRUNC``.
    """

    import os
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "r.json"
        stale = Path(tmp) / f".r.json.{os.getpid()}.tmp"
        stale.write_bytes(b"{stale")
        materialize._write_json_atomic(path, {"a": 1}, text="sorted_lf",
                                       tmp="pid", fsync=False)
        assert _read(path) == b'{"a": 1}\n'


def test_bytes_twin_keeps_the_pool_policy():
    """``pool._write_bytes_atomic``'s contract beside the owner."""

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "blob"
        materialize.write_bytes_atomic(path, b"\x00\x01")
        assert _read(path) == b"\x00\x01"
        assert [p.name for p in Path(tmp).iterdir()] == ["blob"]
