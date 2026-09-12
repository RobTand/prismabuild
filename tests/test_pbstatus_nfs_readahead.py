"""The host readahead readiness check, against fake mount tables and sysfs.

Issue #523 measured both Spark clients at ``read_ahead_kb=1024`` and proposed
16 MiB, and nothing in the fleet reported what the window actually is. The
check that closes that gap is a *report*, and the tests that matter here are
the ones that pin it as one: on every input -- a low window, a high window, a
box that is not an NFS client at all, and a window that cannot be read -- the
census still completes, the exit status is unchanged, and nothing raises.

Nothing here touches a real mount, a real sysfs tree, or the installed helper
on this box. The mount tables are the same fixtures ``test_nfs_readahead.py``
builds, and the helper is loaded from the repository by path.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pbstatus  # noqa: E402

HELPER = REPOSITORY / "fleet/storage/nfs_readahead.py"

AUTOFS = "52 36 0:40 / /mnt/shared rw - autofs systemd-1 rw\n"


def nfs(bdi="0:64", mount_id="392", source="10.100.98.3:/storage_pool/shared"):
    return f"{mount_id} 52 {bdi} / /mnt/shared rw - nfs4 {source} rw\n"


@pytest.fixture
def host(tmp_path):
    """A private mount table and sysfs tree, both writable by the test."""

    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(AUTOFS + nfs())
    bdi_root = tmp_path / "bdi"
    for bdi in ("0:40", "0:64", "8:0"):
        directory = bdi_root / bdi
        directory.mkdir(parents=True)
        (directory / "read_ahead_kb").write_text("1024\n")
    return mountinfo, bdi_root


def read(host, **overrides):
    mountinfo, bdi_root = host
    return pbstatus.nfs_readahead_reading(
        helper_path=overrides.pop("helper_path", HELPER),
        mountinfo=overrides.pop("mountinfo", mountinfo),
        bdi_root=overrides.pop("bdi_root", bdi_root), **overrides)


def test_below_the_recommendation_warns_and_names_both_numbers(host):
    reading = read(host)
    assert reading["state"] == "below_recommended"
    assert reading["warning"] is True
    assert reading["read_ahead_kib"] == 1024
    assert reading["recommended_kib"] == 16384
    assert reading["bdi"] == "0:64"
    assert reading["source"] == "10.100.98.3:/storage_pool/shared"
    # The operator has to be able to act on the line without reading the code.
    assert "1024" in reading["note"] and "16384" in reading["note"]
    assert "nothing is refused" in reading["note"]


@pytest.mark.parametrize("window", ["16384", "32768"])
def test_at_or_above_the_recommendation_does_not_warn(host, window):
    mountinfo, bdi_root = host
    (bdi_root / "0:64/read_ahead_kb").write_text(f"{window}\n")
    reading = read(host)
    assert reading["state"] == "ok"
    assert reading["warning"] is False
    assert reading["read_ahead_kib"] == int(window)


def test_inspection_writes_nothing(host):
    """The check may only ever read; the window is an operator's to change."""

    mountinfo, bdi_root = host
    before = {path: path.read_text() for path in bdi_root.glob("*/read_ahead_kb")}
    read(host)
    assert {path: path.read_text() for path in bdi_root.glob("*/read_ahead_kb")} == before


@pytest.mark.parametrize("table", [
    "",                                   # nothing mounted at /mnt/shared
    AUTOFS,                               # the automount stub and no export
    nfs().replace("nfs4", "ext4"),        # a local filesystem there instead
    nfs(source="server:/other"),          # some other export
    AUTOFS + nfs() + nfs("0:88"),         # ambiguous
])
def test_not_an_nfs_client_is_informational_and_never_warns(host, table):
    """dl380g10 is the storage server, and this is what it reads as."""

    mountinfo, bdi_root = host
    mountinfo.write_text(table)
    reading = read(host)
    assert reading["state"] == "not_nfs_client"
    assert reading["warning"] is False
    assert reading["read_ahead_kib"] is None
    assert reading["note"].startswith("host storage: /mnt/shared is not")


def test_unreadable_sysfs_is_a_note_not_an_exception(host):
    mountinfo, bdi_root = host
    (bdi_root / "0:64/read_ahead_kb").unlink()
    reading = read(host)
    assert reading["state"] == "unreadable"
    assert reading["warning"] is False
    assert "FileNotFoundError" in reading["note"]


def test_a_non_numeric_window_reads_as_unreadable_not_as_a_wrong_mount(host):
    """The two failure classes must not collapse into each other."""

    mountinfo, bdi_root = host
    (bdi_root / "0:64/read_ahead_kb").write_text("not a number\n")
    reading = read(host)
    assert reading["state"] == "unreadable"
    assert reading["warning"] is False


def test_a_generation_without_the_helper_reports_unavailable(host, tmp_path):
    reading = read(host, helper_path=tmp_path / "absent/nfs_readahead.py")
    assert reading["state"] == "unavailable"
    assert reading["warning"] is False
    assert reading["read_ahead_kib"] is None
    assert "could not be loaded" in reading["note"]


def test_a_helper_that_raises_on_import_does_not_reach_the_caller(host, tmp_path):
    broken = tmp_path / "broken_helper.py"
    broken.write_text("raise RuntimeError('helper is broken')\n")
    reading = read(host, helper_path=broken)
    assert reading["state"] == "unavailable"
    assert reading["warning"] is False
    assert "RuntimeError" in reading["note"]


def test_the_default_helper_path_is_inside_the_runtime_generation():
    """A published generation carries the helper; nothing resolves a checkout."""

    assert pbstatus.NFS_READAHEAD_HELPER == "fleet/storage/nfs_readahead.py"
    assert (pbstatus.RUNTIME_ROOT / pbstatus.NFS_READAHEAD_HELPER).is_file()


def test_the_default_reading_on_this_box_never_raises():
    """Whatever this box is -- client, server, container -- it answers."""

    reading = pbstatus.nfs_readahead_reading()
    assert reading["check"] == "nfs_readahead"
    assert reading["state"] in {
        "ok", "below_recommended", "not_nfs_client", "unreadable",
        "unavailable"}
    assert isinstance(reading["warning"], bool)


def _screen(tmp_path, capsys, *extra, expected_code=0):
    """One whole run of the screen, with no controller and an empty queue.

    The SLURM transport with three absent binaries is the same shape
    ``test_pbstatus_queue_root`` uses: it exits 0 and files its notes, so an
    exit status or a ``complete`` that moves here moved because of the
    readahead reading and nothing else.
    """

    absent = str(tmp_path / "absent")
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        (tmp_path / "pb-queue" / state).mkdir(parents=True, exist_ok=True)
    argv = ["--transport", "slurm", "--sinfo", absent, "--squeue", absent,
            "--scontrol", absent, "--queue-root", str(tmp_path / "pb-queue"),
            *extra]
    assert pbstatus.main(argv) == expected_code
    return capsys.readouterr()


def test_a_low_window_prints_one_stderr_line_and_changes_no_exit_status(
        tmp_path, capsys, monkeypatch):
    """The whole point: it reports, and the census is untouched."""

    monkeypatch.setattr(pbstatus, "nfs_readahead_reading", lambda **_: {
        "check": "nfs_readahead", "mount": "/mnt/shared",
        "state": "below_recommended", "read_ahead_kib": 1024,
        "recommended_kib": 16384, "bdi": "0:64", "source": "s:/shared",
        "warning": True, "note": "host storage: read_ahead_kb=1024 is low"})
    captured = _screen(tmp_path, capsys, "--json")
    assert "host storage: read_ahead_kb=1024 is low" in captured.err
    payload = json.loads(captured.out)
    assert payload["host_storage"]["state"] == "below_recommended"
    # The reading is not a census failure and must not be filed as one.
    assert payload["complete"] is True
    assert payload["timed_out_sections"] == []
    assert payload["unavailable_sections"] == []
    assert payload["scheduler"] == [] or all(
        "host storage" not in note for note in payload["scheduler"])


@pytest.mark.parametrize("state", ["ok", "not_nfs_client", "unavailable"])
def test_states_with_nothing_to_act_on_stay_off_stderr(
        tmp_path, capsys, monkeypatch, state):
    monkeypatch.setattr(pbstatus, "nfs_readahead_reading", lambda **_: {
        "check": "nfs_readahead", "mount": "/mnt/shared", "state": state,
        "read_ahead_kib": None, "recommended_kib": 16384, "bdi": None,
        "source": None, "warning": False, "note": "host storage: quiet"})
    captured = _screen(tmp_path, capsys, "--json")
    assert "host storage" not in captured.err
    # Quiet on stderr, still complete in the object a wrapper reads.
    assert json.loads(captured.out)["host_storage"]["state"] == state


def test_an_unreadable_window_on_a_client_is_worth_a_line(
        tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(pbstatus, "nfs_readahead_reading", lambda **_: {
        "check": "nfs_readahead", "mount": "/mnt/shared",
        "state": "unreadable", "read_ahead_kib": None,
        "recommended_kib": 16384, "bdi": "0:64", "source": "s:/shared",
        "warning": False, "note": "host storage: could not be read"})
    captured = _screen(tmp_path, capsys)
    assert "host storage: could not be read" in captured.err
