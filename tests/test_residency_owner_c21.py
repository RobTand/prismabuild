"""Original-source byte replay and the concrete residency filing boundary."""
from dataclasses import replace
from pathlib import Path
import shutil
import subprocess
import threading
import types

import pytest

from prismabuild import core, residency_plan, submission
import pbrun
from test_pbrun_residency_stage_submission import _price_receipt, RAM_TIER
from test_the_handoff_owns_its_caller_boundaries import _stage_build

BASE = "a8ba9c0c7beeeb379908924a60c6933e997c8df6"
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def original_submission():
    source = subprocess.check_output([
        "git", "-C", str(ROOT), "show", BASE + ":src/prismabuild/submission.py"])
    module = types.ModuleType("prismabuild._c21_original_submission")
    module.__file__ = str(ROOT / "src/prismabuild/submission.py")
    exec(compile(source, BASE + ":submission.py", "exec"), module.__dict__)
    return module


@pytest.mark.parametrize("ram", ["off", "auto"])
@pytest.mark.parametrize("share", ["off", "auto"])
@pytest.mark.parametrize("measured", [False, True])
def test_original_and_owner_file_identical_plan_and_request_bytes(
        tmp_path, monkeypatch, original_submission, ram, share, measured):
    built = _stage_build(tmp_path, monkeypatch)
    queue, cas, template, key = (built[name] for name in ("queue", "cas", "template", "key"))
    request = replace(pbrun.request_from_args(built["args"]),
                      residency_ram=ram, residency_share=share)
    store = pbrun.configured_store()
    if ram == "auto":
        queue.announce_tier({**built["tier"], "tier_id": RAM_TIER, "tier": "ram",
            "mountpoint": str(tmp_path / "ram"), "window_gib": 8,
            "promotion_chunk_gib": 2})
    if measured:
        _price_receipt(queue, "a" * 64, delivered_mb_s=20)
    original = original_submission.residency_stage_rows(template,
        consumer_action_key=key, tier=built["tier"], args=request,
        queue=queue, cas=cas, store=store)
    residency_plan.seal_window(queue, original["plan"])
    expected = queue.records.residency_plan_path(key).read_bytes()
    requests = {path.relative_to(cas.root): path.read_bytes()
                for path in (cas.root / "requests").rglob("*.json")}
    filing = residency_plan.read_filed(queue, key)[1]
    assert residency_plan.reap(queue, key, plan=original["plan"], filing=filing) is not None
    shared = residency_plan.registry_root(queue) / residency_plan.SHARED
    if shared.exists():
        shutil.rmtree(shared)
    with residency_plan.consumer_filing(queue, key, template=template,
            options=submission.filing_options(request),
            publication=submission.row_publication(request, store), cas=cas) as filed:
        assert core._canonical_bytes(filed.plan) == core._canonical_bytes(original["plan"])
        assert queue.records.residency_plan_path(key).read_bytes() == expected
        assert filed.residency == original["residency"]
        assert filed.retired == ()
        assert not filed.reused
    assert {path.relative_to(cas.root): path.read_bytes()
            for path in (cas.root / "requests").rglob("*.json")} == requests
    # A later price observation reuses the exact first-writer filing.
    _price_receipt(queue, "b" * 64, delivered_mb_s=300)
    with residency_plan.consumer_filing(queue, key, template=template,
            options=submission.filing_options(request),
            publication=submission.row_publication(request, store), cas=cas) as reused:
        assert reused.reused
        assert queue.records.residency_plan_path(key).read_bytes() == expected


def test_filing_lock_covers_real_consumer_row_publication(tmp_path, monkeypatch):
    built = _stage_build(tmp_path, monkeypatch)
    queue, cas, template, key = (built[name] for name in ("queue", "cas", "template", "key"))
    request = pbrun.request_from_args(built["args"])
    publication = submission.row_publication(request, pbrun.configured_store())
    action = submission.seal(template)
    cas.publish_action_request(action)
    observed = []
    with residency_plan.consumer_filing(queue, key, template=template,
            options=submission.filing_options(request), publication=publication,
            cas=cas) as filed:
        def reaper():
            with queue.action_transition_lock(key, blocking=False) as acquired:
                observed.append(bool(acquired))
                if acquired:
                    observed.append(residency_plan.reap(queue, key))
        worker = threading.Thread(target=reaper)
        worker.start()
        worker.join(10)
        assert not worker.is_alive()
        assert observed == [False]
        row = publication.row(action, queue=queue)
        row["residency"] = filed.residency
        queue.publish(**row)
    assert queue.records.item_path("ready", key).exists()
    assert residency_plan.reap(queue, key) is None
    assert queue.records.residency_plan_path(key).exists()


def test_registry_observations_keep_foreign_names_and_unknown_distinct(tmp_path):
    from prismabuild.pool import PoolQueue
    queue = PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    root = residency_plan.registry_root(queue)
    (root / "foreign.json").write_text("{}")
    (root / ("g" * 64 + ".json")).write_text("{}")
    assert residency_plan.inspection_names(queue) == ["foreign.json", "g" * 64 + ".json"]
    assert residency_plan.filed_consumers(queue) == []
    assert residency_plan.filed_consumer_candidates(queue) == ["g" * 64]
    retired = root / residency_plan.SUPERSEDED
    retired.mkdir()
    (retired / "foreign.json").write_text("{}")
    assert residency_plan.active_supersessions(queue) is None
    assert residency_plan.retired_consumers(queue) == {"foreign"}


def test_native_runtime_records_the_actual_executable_and_source(capsys):
    import hashlib
    import json
    import os
    import sys
    executable = Path(sys.executable).resolve(strict=True)
    executable_bytes = executable.read_bytes()
    owner = Path(residency_plan.__file__).resolve(strict=True)
    owner_bytes = owner.read_bytes()
    native = {name: os.environ.get(name) for name in
              ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS")}
    record = {"version": sys.version, "version_info": list(sys.version_info[:3]),
              "executable": str(executable), "invoked": sys.executable,
              "executable_bytes": len(executable_bytes),
              "executable_sha256": hashlib.sha256(executable_bytes).hexdigest(),
              "owner": str(owner), "owner_bytes": len(owner_bytes),
              "owner_sha256": hashlib.sha256(owner_bytes).hexdigest(),
              "affinity": sorted(os.sched_getaffinity(0)), "native_threads": native}
    with capsys.disabled():
        print("c21-runtime: " + json.dumps(record), flush=True)
    assert owner.is_relative_to(ROOT / "src")
    assert set(native.values()) == {"1"}
