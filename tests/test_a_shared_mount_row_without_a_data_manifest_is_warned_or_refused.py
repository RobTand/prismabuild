"""A row that will read the shared mount cold should say so before it runs.

The storage prewarm can only make resident the bytes a ``data_manifest``
names, so a row that reads the shared mount without one runs cold and nothing
reports it.  ``pbcampaign`` cannot see what a command opens while it runs, so
it offers two policies:

* By default it warns when a row *declares* a shared-mount path -- an absolute
  path at or under the fleet's shared mount in ``argv`` or ``env`` -- and
  names no manifest.  The scan is best-effort: it sees only the declaration,
  says the row *may* read those paths cold, and a path the command only
  writes is a false positive.
* ``--require-data-manifest`` does not depend on the scan at all.  It refuses
  the whole manifest before its first row is sealed unless *every* row (and a
  logical request's common half) carries a nonblank ``data_manifest``.  That
  is how a producer whose reads this tool cannot see -- a script that
  assembles paths at run time -- opts into declaring them reliably.

``cwd`` is deliberately not scanned: ``pbrun`` snapshots the checkout and the
action reads a box-local materialization of it, so a checkout on the shared
mount is not a declared data read.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import decomposition as dc, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbcampaign  # noqa: E402

from test_a_data_manifest_names_bytes_exactly import _manifest as _data_manifest  # noqa: E402
from test_a_decomposed_campaign_closes_on_an_exact_cover import _request as _cover_request  # noqa: E402
from test_decomposition_refuses_before_it_publishes_anything import (  # noqa: E402
    _common as _logical_common, _request as _logical_request,
)
from test_pbcampaign import _drain  # noqa: E402
from test_pbrun_detach import _checkout, _queue  # noqa: E402


@pytest.fixture()
def fleet_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return _checkout(tmp_path), _queue(tmp_path)


def _manifest(tmp_path: Path, rows) -> str:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return str(path)


def _row(work: Path, script: str, **fields) -> dict:
    return {"argv": ["/bin/bash", "-lc", script], "cwd": str(work), **fields}


def _submitted(captured) -> list[dict]:
    return [json.loads(line) for line in captured.out.splitlines() if line.strip()]


def _written_manifest(tmp_path: Path) -> Path:
    path = tmp_path / "data-manifest.json"
    path.write_text(json.dumps(_data_manifest()), encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# The warning, at submission
# --------------------------------------------------------------------------

def test_a_shared_mount_row_without_a_manifest_warns_when_it_is_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths, capsys,
) -> None:
    """A declaration is enough to warn on, and the warning names its field."""

    work, queue = fleet_paths
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)
    manifest = _manifest(tmp_path, [
        _row(work, "printf clean"),
        _row(work, f"python train.py --model {shared}/models/M/weight.safetensors"),
    ])

    assert pbcampaign.main(["--transport", "pool", "--detach", manifest]) == 0
    printed = capsys.readouterr()
    assert [line["status"] for line in _submitted(printed)] == [
        "submitted", "submitted"], printed.out
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 2

    assert "WARNING row 1" in printed.err, printed.err
    assert "data_manifest" in printed.err, printed.err
    assert "argv[2]" in printed.err, printed.err
    # The scan sees a declaration, not a read: the warning says "may".
    assert "may read" in printed.err, printed.err
    assert "WARNING row 0" not in printed.err, printed.err


def test_a_row_that_names_its_bytes_is_not_warned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths, capsys,
) -> None:
    """The warning is about a gap, not about the shared mount by itself."""

    work, _queue_unused = fleet_paths
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)
    manifest = _manifest(tmp_path, [
        _row(work, f"python train.py --model {shared}/models/M",
             data_manifest=str(_written_manifest(tmp_path))),
    ])

    assert pbcampaign.main(["--transport", "pool", "--detach", manifest]) == 0
    printed = capsys.readouterr()
    assert [line["status"] for line in _submitted(printed)] == ["submitted"]
    assert "WARNING" not in printed.err, printed.err


def test_a_cache_hit_is_not_warned_about_repeating_a_cold_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths, capsys,
) -> None:
    """Re-running a manifest is free and silent for a row that already ran.

    A warning on a cache hit would be a warning about a read that is not
    happening; the gap is worth reporting once, when the row is published.
    """

    work, queue = fleet_paths
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)
    # The shared path is a harmless printf argument: the command is real, so
    # the row executes and a receipt exists for the re-run to hit.
    manifest = _manifest(tmp_path, [
        _row(work, f"printf ok {shared}/inputs/x.pt"),
    ])

    stop = threading.Event()
    worker = threading.Thread(target=_drain, args=(queue, 1, stop))
    worker.start()
    try:
        assert pbcampaign.main(["--transport", "pool", manifest]) == 0
    finally:
        stop.set()
        worker.join(timeout=60)
    assert "WARNING" in capsys.readouterr().err

    assert pbcampaign.main(["--transport", "pool", "--detach", manifest]) == 0
    printed = capsys.readouterr()
    assert [line["status"] for line in _submitted(printed)] == ["cache_hit"]
    assert "WARNING" not in printed.err, printed.err


# --------------------------------------------------------------------------
# The preflight refusal
# --------------------------------------------------------------------------

def test_require_data_manifest_refuses_a_row_with_nothing_visible_to_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths,
) -> None:
    """The flag is the opt-in for reads this tool cannot see.

    A producer that assembles its paths at run time has no visible
    declaration, so the default scan has nothing to act on.  The flag is
    stricter than the scan on purpose: it demands a manifest from every row.
    """

    work, queue = fleet_paths
    monkeypatch.setattr(pbrun, "SHARED_ROOT", tmp_path / "shared")
    manifest = _manifest(tmp_path, [_row(work, "python train.py --config local.json")])

    with pytest.raises(SystemExit) as refused:
        pbcampaign.main([
            "--transport", "pool", "--detach", "--require-data-manifest",
            manifest,
        ])
    message = str(refused.value)
    assert "row 0" in message and "data_manifest" in message, message
    assert not list(queue.dir(pool.READY).glob("*.json"))


def test_require_data_manifest_refuses_before_any_row_is_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths,
) -> None:
    """The refusal is of the whole manifest, and it names the row to fix."""

    work, queue = fleet_paths
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)
    manifest = _manifest(tmp_path, [
        _row(work, "printf clean",
             data_manifest=str(_written_manifest(tmp_path))),
        _row(work, f"python train.py --data {shared}/inputs/x.pt"),
        _row(work, "printf also-clean"),
    ])

    with pytest.raises(SystemExit) as refused:
        pbcampaign.main([
            "--transport", "pool", "--detach", "--require-data-manifest",
            manifest,
        ])
    message = str(refused.value)
    assert "row 1" in message and "data_manifest" in message, message
    assert "--require-data-manifest" in message, message
    # The rows before the flagged one were not sealed either: the refusal is
    # the manifest's, and no read can have happened cold.
    assert not list(queue.dir(pool.READY).glob("*.json"))


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_require_data_manifest_treats_a_blank_manifest_as_missing(
    tmp_path: Path, fleet_paths, blank: str,
) -> None:
    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [_row(work, "printf ok", data_manifest=blank)])

    with pytest.raises(SystemExit) as refused:
        pbcampaign.main([
            "--transport", "pool", "--detach", "--require-data-manifest",
            manifest,
        ])
    assert "row 0" in str(refused.value), str(refused.value)
    assert not list(queue.dir(pool.READY).glob("*.json"))


def test_require_data_manifest_refuses_the_windowed_prefix_before_any_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths,
) -> None:
    """A bounded window is still a preflight: no prefix is ever published."""

    work, queue = fleet_paths
    monkeypatch.setattr(pbrun, "SHARED_ROOT", tmp_path / "shared")
    manifest = _manifest(tmp_path, [
        _row(work, "printf clean",
             data_manifest=str(_written_manifest(tmp_path))),
        _row(work, "printf missing"),
    ])

    with pytest.raises(SystemExit) as refused:
        pbcampaign.main([
            "--transport", "pool", "--max-inflight", "1", "--wait-s", "0",
            "--require-data-manifest", manifest,
        ])
    assert "row 1" in str(refused.value), str(refused.value)
    assert not list(queue.dir(pool.READY).glob("*.json"))


def test_require_data_manifest_accepts_a_row_that_names_its_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths, capsys,
) -> None:
    """With a manifest attached, the same row submits normally."""

    work, _queue_unused = fleet_paths
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)
    manifest = _manifest(tmp_path, [
        _row(work, f"python train.py --data {shared}/inputs/x.pt",
             data_manifest=str(_written_manifest(tmp_path))),
    ])

    assert pbcampaign.main([
        "--transport", "pool", "--detach", "--require-data-manifest", manifest,
    ]) == 0
    assert [line["status"] for line in _submitted(capsys.readouterr())] == [
        "submitted"]


def test_require_data_manifest_refuses_a_logical_requests_common_before_it_is_cut(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths,
) -> None:
    """One request's common half is validated with the same rule and timing."""

    work, queue = fleet_paths
    monkeypatch.setattr(pbrun, "SHARED_ROOT", tmp_path / "shared")
    request = _logical_request(common=_logical_common(
        data_manifest=None,
        argv=["python", "collect.py", dc.TASK_BATCH_PLACEHOLDER,
              "--config", "local.json"],
    ))
    manifest = _manifest(tmp_path, request)

    with pytest.raises(SystemExit) as refused:
        pbcampaign.main([
            "--transport", "pool", "--detach", "--require-data-manifest",
            manifest,
        ])
    message = str(refused.value)
    assert "common" in message and "data_manifest" in message, message
    assert not list(queue.dir(pool.READY).glob("*.json"))
    assert not (tmp_path / "cas" / pbcampaign.DECOMPOSITIONS).exists()


def test_require_data_manifest_passes_a_common_half_that_names_its_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default logical request already names its bytes and is accepted."""

    monkeypatch.setattr(pbrun, "SHARED_ROOT", tmp_path / "shared")
    request = _logical_request()
    manifest = _manifest(tmp_path, request)

    assert pbcampaign.load_manifest(
        manifest, transport="pool", require_data_manifest=True,
    ) == dc.validate_logical_request(request)


# --------------------------------------------------------------------------
# What the declaration scan sees
# --------------------------------------------------------------------------

def test_the_scan_reads_flag_values_path_lists_shell_words_and_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)

    assert pbcampaign.shared_mount_read_evidence(
        {"argv": ["python", f"--model={shared}/M"], "env": {}}) == ["argv[1]"]
    assert pbcampaign.shared_mount_read_evidence(
        {"argv": ["python"], "env": {"PYTHONPATH": f"src:{shared}/lib"}}
    ) == ["env['PYTHONPATH']"]
    assert pbcampaign.shared_mount_read_evidence(
        {"argv": ["bash", "-lc", f'python x.py --data "{shared}/a b/c.pt"']}
    ) == ["argv[2]"]
    # The root itself is a path on the mount, not only something under it.
    assert pbcampaign.shared_mount_read_evidence(
        {"argv": ["python", str(shared)]}) == ["argv[1]"]


def test_the_scan_does_not_confuse_a_sibling_or_a_relative_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)

    assert pbcampaign.shared_mount_read_evidence(
        {"argv": ["python", f"--data={shared}ish/x.pt"]}) == []
    assert pbcampaign.shared_mount_read_evidence(
        {"argv": ["python", "train.py", "--data", "inputs/x.pt"]}) == []


def test_a_shared_checkout_is_not_a_declared_data_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``pbrun`` snapshots ``cwd``, so it is not scanned for shared reads."""

    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)
    assert pbcampaign.shared_mount_read_evidence({
        "argv": ["python", "train.py"],
        "cwd": str(shared / "work"),
        "env": {},
    }) == []


# --------------------------------------------------------------------------
# The decomposed request warns once, when a child will actually read cold
# --------------------------------------------------------------------------

def test_a_logical_requests_common_half_is_warned_once_when_children_are_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths, capsys,
) -> None:
    work, _queue_unused = fleet_paths
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SHARED_ROOT", shared)
    request = _cover_request(work)
    request["common"]["data_manifest"] = None
    request["common"]["argv"] = [
        sys.executable, "-c", "print('batch')", dc.TASK_BATCH_PLACEHOLDER,
        f"--model={shared}/M",
    ]
    manifest = _manifest(tmp_path, request)

    assert pbcampaign.main(["--transport", "pool", "--detach", manifest]) == 0
    printed = capsys.readouterr()
    assert _submitted(printed), "the request was not cut into children"
    warnings = [line for line in printed.err.splitlines() if "WARNING" in line]
    assert len(warnings) == 1, printed.err
    assert "common half" in warnings[0], printed.err
