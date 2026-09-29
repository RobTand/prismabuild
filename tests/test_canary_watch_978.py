"""#978: an active generation with no canary verdict is seen and gets a run."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))

import pbstatus  # noqa: E402


def _fleet(tmp_path: Path, name: str = "gen-1") -> tuple[Path, Path]:
    store = tmp_path / "runtime-generations"
    generation = store / name
    generation.mkdir(parents=True)
    (generation / "RUNTIME_VERSION.json").write_text(json.dumps({"commit": "abc123"}))
    link = tmp_path / "repo"
    link.symlink_to(generation)
    return link, store


def _record(store: Path, name: str, status: str, *, code=0, recorded=1000.0):
    (store / f"{name}.canary.json").write_text(json.dumps({
        "generation": name, "canary_status": status, "canary_exit": code,
        "recorded_unix": recorded,
    }))


def test_activated_generation_without_a_record_is_reported_missing(tmp_path):
    link, _store = _fleet(tmp_path)
    summary = pbstatus.read_canary_summary(link)
    assert summary["state"] == "missing"
    assert pbstatus.canary_lines(summary) == [
        "missing canary verdict for generation gen-1"]


def test_verified_record_is_a_verdict(tmp_path):
    link, store = _fleet(tmp_path)
    _record(store, "gen-1", "verified")
    summary = pbstatus.read_canary_summary(link, now=1001.0)
    assert summary["state"] == "verified"
    assert "missing" not in pbstatus.canary_lines(summary)[0]


def test_pending_record_that_never_reported_is_missing(tmp_path):
    link, store = _fleet(tmp_path)
    _record(store, "gen-1", "pending", code=None)
    fresh = pbstatus.read_canary_summary(link, now=1060.0)
    stale = pbstatus.read_canary_summary(
        link, now=1000.0 + pbstatus.CANARY_PENDING_STALE_S + 1)
    assert fresh["state"] == "pending"
    assert stale["state"] == "pending_stale"
    assert pbstatus.canary_lines(stale)[0].startswith(
        "missing canary verdict for generation gen-1")


def test_record_for_another_generation_is_not_a_verdict(tmp_path):
    link, store = _fleet(tmp_path)
    (store / "gen-1.canary.json").write_text(json.dumps({
        "generation": "gen-0", "canary_status": "verified"}))
    assert pbstatus.read_canary_summary(link)["state"] == "unreadable"


def _watch(link, runner, *extra):
    import pbcanary_watch
    return pbcanary_watch.main(["--repo-link", str(link), *extra], runner=runner)


def test_watcher_runs_the_canary_for_a_missing_generation(tmp_path):
    link, store = _fleet(tmp_path)
    calls = []
    assert _watch(link, lambda gen, extra: calls.append(gen) or 0) == 0
    assert calls == ["gen-1"]
    record = json.loads((store / "gen-1.canary.json").read_text())
    assert record["canary_status"] == "verified"
    assert record["commit"] == "abc123"
    assert pbstatus.read_canary_summary(link)["state"] == "verified"


@pytest.mark.parametrize("code,status", [(1, "failed"), (2, "not_run")])
def test_watcher_records_the_drivers_exit(tmp_path, code, status):
    link, store = _fleet(tmp_path)
    assert _watch(link, lambda gen, extra: code) == code
    assert json.loads((store / "gen-1.canary.json").read_text())[
        "canary_status"] == status


def test_watcher_skips_a_generation_that_has_a_verdict(tmp_path):
    link, store = _fleet(tmp_path)
    _record(store, "gen-1", "verified")
    calls = []
    assert _watch(link, lambda gen, extra: calls.append(gen) or 0) == 0
    assert calls == []


def test_nightly_runs_even_with_a_verdict(tmp_path):
    link, store = _fleet(tmp_path)
    _record(store, "gen-1", "verified")
    calls = []
    assert _watch(link, lambda gen, extra: calls.append(gen) or 0, "--nightly") == 0
    assert calls == ["gen-1"]


def test_a_crashing_driver_does_not_leave_the_record_pending(tmp_path):
    link, store = _fleet(tmp_path)

    def boom(gen, extra):
        raise RuntimeError("driver crashed")

    with pytest.raises(RuntimeError):
        _watch(link, boom)
    assert json.loads((store / "gen-1.canary.json").read_text())[
        "canary_status"] == "failed"


def test_canary_section_is_in_the_status_document():
    src = (ROOT / "tools/fleet/pbstatus.py").read_text()
    assert '"canary": canary' in src and 'print("== canary")' in src
