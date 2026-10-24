"""Offline acceptance of #2183's *completed* private-pool smoke handoff.

The implementer supplies the smoke and docs/evidence/pb2183-retirement.json.
Small raw observation/receipt/log copies live beside that report. These checks
recount observations and verify custody bytes; they do not authenticate an
execution merely because JSON claims it happened. Review must additionally
verify retained run_tests evidence, canonical lifecycle calls and CAS reads.
An unavailable arm is honest reporting, but is NOT passing smoke acceptance.
No live fleet access, historical Git objects, submission or extra execution.
"""
from __future__ import annotations

import json
from pathlib import Path
import re

import pytest

from prismabuild.core import canonical_sha256, raw_sha256
from prismabuild.pool import ORPHAN_PASSES_SWEEP_LIMIT


ROOT = Path(__file__).resolve().parents[1]
SMOKE = "tests/test_measurement_election_retirement_smoke_1507.py"
HANDOFF = ROOT / "docs/evidence/pb2183-retirement.json"


def _retirement_handoff_2183(arm):
    assert HANDOFF.is_file(), (
        "#2183 has no completed private-pool retirement smoke evidence: "
        "supply docs/evidence/pb2183-retirement.json after the canonical W1 "
        "lifecycle smoke; existing timing/cursor tests are not retirement proof"
    )
    report = json.loads(HANDOFF.read_bytes())
    assert report["host_identity_simulated"] is True
    assert report["deployed_two_host_evidence"] is False
    assert report["limitations"], "disclose private-host simulation and unavailable observations"
    assert (ROOT / SMOKE).is_file(), "supply the executable isolated smoke, not just a report"
    assert report["smoke_source_sha256"] == raw_sha256((ROOT / SMOKE).read_bytes())
    run = report["runs"][arm]
    assert run["completed"] is True, f"{arm}: unavailable/queued is not a completed smoke"
    assert re.fullmatch(r"[0-9a-f]{40}", run["source_revision"])
    assert SMOKE in run["command"] and '--basetemp="$TMPDIR/pytest"' in run["command"]
    assert "Test evidence" in run["test_evidence"]
    return run


def _retirement_object_2183(record):
    """Copies retain original addresses; hashes describe actual readable bytes."""
    assert record["reference"].strip()
    assert record["read_operation"].strip(), "record the successful original-object read"
    assert re.fullmatch(r"[0-9a-f]{64}", record["sha256"])
    relative = Path(record["copy"])
    assert not relative.is_absolute() and ".." not in relative.parts
    path = ROOT / relative
    assert path.is_file(), f"missing independently readable retained bytes: {relative}"
    data = path.read_bytes()
    assert len(data) == record["bytes"]
    assert raw_sha256(data) == record["sha256"]
    return data


def _retirement_observations_2183(run):
    return json.loads(_retirement_object_2183(run["observations"]))


@pytest.mark.parametrize("arm,version", [("x86", [3, 14]), ("arm", [3, 12])])
def test_retirement_smoke_has_completed_admitted_execution_and_real_cas(arm, version):
    run = _retirement_handoff_2183(arm)
    outer = run["outer_attempt"]
    assert re.fullmatch(r"[0-9a-f]{64}", outer["action_key"])
    assert outer["attempt"] and outer["returncode"] == 0
    for name in ("receipt", "log", "terminal_record"):
        _retirement_object_2183(outer[name])
    stdout = _retirement_object_2183(run["stdout"]).decode()
    runtime = [json.loads(line.split(": ", 1)[1]) for line in stdout.splitlines()
               if line.startswith("prismabuild-test-runtime: ")]
    assert runtime and runtime[-1]["complete"] is True
    assert runtime[-1]["version"][:2] == version
    assert runtime[-1]["exitstatus"] == 0
    assert runtime[-1]["selected_source_sha256"][SMOKE] == raw_sha256((ROOT / SMOKE).read_bytes())
    facts = _retirement_observations_2183(run)
    success = facts["successful_measurement"]
    assert success["outcome"]["status"] == "executed"
    assert success["outcome"]["returncode"] == 0
    assert success["claim"]["action_key"] == success["action_key"]
    assert success["claim"]["published_unix"] == success["election"]["published_unix"]
    assert success["terminal"]["action_key"] == success["action_key"]
    assert success["holder_tokens_after_claim"], "exercise the real resource ledger"
    assert success["holder_tokens_after_finish"] == []
    receipt = json.loads(_retirement_object_2183(success["execution_receipt"]))
    assert receipt["action_key"] == success["action_key"]
    assert receipt["receipt_sha256"] == canonical_sha256(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"})
    payload = _retirement_object_2183(success["payload"])
    assert payload, "read the real tiny CPU result"
    assert receipt["result"]["sha256"] == success["payload"]["sha256"]
    assert receipt["result"]["bytes"] == len(payload)
    assert success["execution_receipt"]["reference"] != success["payload"]["reference"]
    _retirement_object_2183(success["log"])
    withdrawn = facts["withdrawn_measurement"]
    assert withdrawn["withdrawal"]["action_key"] == withdrawn["action_key"]
    assert withdrawn["withdrawal_covers_elected_generation"] is True
    assert withdrawn["election"]["published_unix"] == withdrawn["published_unix"]
    preserved = facts["preserved_objects"]
    assert {entry["kind"] for entry in preserved} >= {"attempt_receipt", "scientific_receipt"}
    for entry in preserved:
        assert _retirement_object_2183(entry["before"]) == _retirement_object_2183(entry["after"])
        assert entry["before"]["reference"] == entry["after"]["reference"]


@pytest.mark.parametrize("arm", ["x86", "arm"])
def test_retirement_smoke_recounts_reclamation_and_persistent_bounded_cursor(arm):
    facts = _retirement_observations_2183(_retirement_handoff_2183(arm))
    controls = facts["controls"]
    assert controls["live"] and controls["unknown"]
    snapshots = facts["snapshots"]
    for stage in ("before", "pre_sweep", "after"):
        files = snapshots[stage]["files"]  # pass filename stem -> original JSON document
        assert all(record["action_key"] == key for key, record in files.items())
        expected = {"pass_files": len(files),
                    "election_files": sum("measurement_reservation" in record for record in files.values()),
                    "live_controls": sum(key in files for key in controls["live"]),
                    "unknown_controls": sum(key in files for key in controls["unknown"])}
        assert snapshots[stage]["counts"] == expected
    before, pre_sweep, after = (set(snapshots[stage]["files"]) for stage in ("before", "pre_sweep", "after"))
    targets = {facts[name]["action_key"] for name in ("successful_measurement", "withdrawn_measurement")}
    assert len(targets) == 2 and targets <= before and not targets & after
    assert all("measurement_reservation" in snapshots["before"]["files"][key] for key in targets)
    assert facts["remaining_target_files"] == []
    assert facts["total_reclaimed"] == len(before - after) >= 2
    assert facts["sweep_limit"] == ORPHAN_PASSES_SWEEP_LIMIT
    assert len(facts["retained_prefix"]) > ORPHAN_PASSES_SWEEP_LIMIT
    assert set(facts["retained_prefix"]) <= pre_sweep & after
    sweep_targets = targets & pre_sweep
    assert sweep_targets, "prove actual sweep retirement, not just claim-time deletion"
    assert max(facts["retained_prefix"]) < min(sweep_targets), "targets must lie beyond retained prefix"
    sweeps = facts["sweeps"]
    assert len(sweeps) >= 2
    assert len({row["owner_epoch"] for row in sweeps}) >= 2, "restart the sweep owner"
    removed = []
    for index, row in enumerate(sweeps):
        removed.extend(row["removed"])
        assert row["removed_count"] == len(row["removed"])
        if row["examined_count"] is None:
            assert row["examined_unavailable_reason"], "do not invent an unexposed counter"
        else:
            assert len(row["removed"]) <= row["examined_count"] <= ORPHAN_PASSES_SWEEP_LIMIT
        if index:
            assert row["cursor_before"] == sweeps[index - 1]["cursor_after"]
    assert len(removed) == len(set(removed))
    assert set(removed) == pre_sweep - after
    assert facts["sweep_reclaimed"] == len(removed) > 0


@pytest.mark.parametrize("arm", ["x86", "arm"])
def test_retirement_smoke_fresh_census_and_live_authority_stay_conservative(arm):
    facts = _retirement_observations_2183(_retirement_handoff_2183(arm))
    controls = facts["controls"]
    after = facts["snapshots"]["after"]["files"]
    assert set(controls["live"] + controls["unknown"]) <= set(after)
    census = facts["fresh_census"]
    assert census["complete"] is True and census["cached"] is False
    assert census["record_count"] > 0 and census["pass_file_count"] == len(after)
    assert census["selection_count"] == len(census["selection_keys"])
    assert set(controls["live"]) <= set(census["selection_keys"])
    assert not {facts[name]["action_key"] for name in
                ("successful_measurement", "withdrawn_measurement")} & set(census["selection_keys"])
    refusal = facts["lower_priority_admission"]
    assert refusal["claimed"] is False
    assert refusal["reason"] == "deferred_for_measurement_reservation"
    assert refusal["blocking_action_key"] in controls["live"]
    assert facts["real_claim_outcomes"], "retain canonical claim/refusal observations"
