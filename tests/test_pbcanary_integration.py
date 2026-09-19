"""Integration seams for the pbcanary fleet canary (RobTand/prismabuild#688).

Pins what the five crew branches only assumed about each other — all
without fleet contact:

* legs 3-4 live under ``tools/fleet/pbcanary_legs`` with the crew-A
  contract (``NAME`` labels, ``build()``, verify arity, worker argv
  pointing at the moved path);
* the driver dispatches all four legs by spec shape (single submit for
  legs 1-3, two-box fanout + three-arg verify for leg 4) and feeds leg-4
  ``digest_a``/``digest_b`` through to the verdict;
* every leg's verify-failure reasons avoid the verdict's precondition
  markers (corrupted digests read exit 1, never exit 2), while the extra
  ``artifact_digest`` key stays verdict-safe;
* the rollout gate's ``run_canary(generation=<name>)`` call matches the
  driver's actual entry, and the CI workflow's ``--generation`` /
  ``--summary-dir`` flags exist on the driver.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import sys
from pathlib import Path

import pytest

FLEET_DIR = Path(__file__).resolve().parents[1] / "tools" / "fleet"
if str(FLEET_DIR) not in sys.path:
    sys.path.insert(0, str(FLEET_DIR))

import pbcanary  # noqa: E402
from pbcanary_legs import leg1, leg2, leg3, leg4  # noqa: E402
from pbcanary_verdict import PRECONDITION_MARKERS, verdict  # noqa: E402

PINNED_IMAGE = "example.org/pb-campaign@sha256:" + "ab" * 32


def _no_markers(reason: str) -> None:
    lowered = reason.lower()
    for marker in PRECONDITION_MARKERS:
        assert marker not in lowered, (marker, reason)


# --- legs 3-4 contract -------------------------------------------------------


def test_leg_names_match_verdict_labels() -> None:
    assert (leg1.NAME, leg2.NAME, leg3.LEG3_NAME, leg4.LEG4_NAME) == (
        "leg-1", "leg-2", "leg-3", "leg-4")


def test_leg_builds_are_green_and_worker_argv_uses_moved_path() -> None:
    for module, name in ((leg3, "leg3"), (leg4, "leg4")):
        spec = module.build()
        assert spec["expected"]
        argv = (spec.get("argv") or spec.get("action", {}).get("argv")
                or spec["actions"][0]["argv"])
        assert argv == [
            "python3", f"tools/fleet/pbcanary_legs/{name}.py", "--run-action"], argv
        assert Path(FLEET_DIR / "pbcanary_legs" / f"{name}.py").is_file()


def test_leg3_manifest_roundtrip_in_tmp(tmp_path: Path) -> None:
    spec = leg3.build()
    chunk_paths = leg3.write_chunk_files(str(tmp_path))
    manifest = leg3.manifest_for_run(chunk_paths, str(tmp_path))
    assert manifest["total_bytes"] == sum(leg3.LEG3_CHUNK_SIZES)
    assert [e["sha256"] for e in manifest["entries"]] == list(
        leg3.LEG3_EXPECTED_CHUNK_SHA256)


def _leg3_ok_receipt() -> tuple[dict, dict]:
    spec = leg3.build()
    envelope = {
        "schema": leg3.LEG3_SCHEMA,
        "leg": 3,
        "chunks": [
            {"path": f"/x/{c['name']}", "bytes": c["size"], "sha256": c["sha256"]}
            for c in spec["expected"]["chunks"]
        ],
        "combined": spec["expected"]["combined"],
        "ok": True,
    }
    raw = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    return spec["expected"], {"stdout": raw + "\n", "returncode": 0,
                              "artifact": raw + "\n"}


def test_leg3_verify_ok_and_corrupted_are_exit1_without_markers() -> None:
    expected, receipt = _leg3_ok_receipt()
    ok, reason = leg3.verify(receipt, expected)
    assert ok, reason
    bad_chunks = dict(receipt)
    bad_chunks["stdout"] = receipt["stdout"].replace(
        expected["combined"], "0" * 64)
    bad_chunks["artifact"] = bad_chunks["stdout"]
    ok, reason = leg3.verify(bad_chunks, expected)
    assert not ok
    _no_markers(reason)


def _leg4_side_receipts() -> tuple[dict, dict, dict]:
    spec = leg4.build()
    from pbcanary_legs.common import canonical_json, deterministic_bytes, sha256_hex
    parts = b"".join(
        deterministic_bytes(bytes.fromhex(e["seed_hex"]), e["bytes"])
        for e in spec["inputs"])
    envelope = {"schema": leg4.LEG4_SCHEMA, "leg": 4,
                "inputs": spec["inputs"],
                "input_digest": sha256_hex(parts)}
    raw = canonical_json(envelope)
    side = {"stdout": raw + "\n", "returncode": 0, "artifact": raw + "\n"}
    return spec["expected"], dict(side), dict(side)


def test_leg4_verify_pairwise_equality_and_tamper() -> None:
    expected, side_a, side_b = _leg4_side_receipts()
    ok, _reason = leg4.verify(side_a, side_b, expected)
    assert ok
    tampered = dict(side_b)
    tampered["stdout"] = side_b["stdout"].replace("4", "5")
    tampered["artifact"] = tampered["stdout"]
    ok, reason = leg4.verify(side_a, tampered, expected)
    assert not ok
    _no_markers(reason)


def test_leg4_verify_arity_is_three_positional() -> None:
    params = list(inspect.signature(leg4.verify).parameters)
    assert params == ["receipt_a", "receipt_b", "expected"]


# --- driver dispatch ---------------------------------------------------------


def test_driver_defaults_to_all_four_legs() -> None:
    assert pbcanary.DEFAULT_LEGS == "leg-1,leg-2,leg-3,leg-4"


def test_driver_entry_accepts_generation_keyword() -> None:
    params = inspect.signature(pbcanary.run_canary).parameters
    assert "generation" in params


def test_driver_help_documents_new_flags(capsys) -> None:
    with pytest.raises(SystemExit):
        pbcanary.main(["--help"])
    out = capsys.readouterr().out
    assert "--generation" in out and "--summary-dir" in out


def test_run_canary_generation_kwarg_is_exit2_without_queue(tmp_path, capsys) -> None:
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    code = pbcanary.run_canary(generation="gen-test",
                               args=_ns(tmp_path, checkout))
    assert code == 2
    assert "queue unreachable" in capsys.readouterr().err


def _ns(tmp_path: Path, checkout: Path):
    import argparse
    return argparse.Namespace(
        legs="leg-1", run_id="test-run", priority=-10,
        checkout=str(checkout), fleet_root=str(tmp_path / "fleet"),
        published_root="/mnt/shared/prismabuild-fleet/repo",
        gpu_image=None, generation=None, summary_dir=None)


def test_fanout_leg_files_single_entry_with_digest_pair(tmp_path, monkeypatch) -> None:
    expected, side_a, side_b = _leg4_side_receipts()
    calls = []

    def fake_execute(paths, **kwargs):
        calls.append(kwargs)
        side = kwargs["side"]
        envelope = dict(side_a if side == "sparky" else side_b)
        envelope["action_key"] = f"key-{side}"
        # Both boxes print the same envelope bytes, so the result blobs
        # (whose digests feed the verdict pair) are identical too.
        return envelope, f"cas/ref-{side}", b"blob-bytes", f"key-{side}"

    monkeypatch.setattr(pbcanary, "_execute_side", fake_execute)
    leg_dir = tmp_path / "leg-4"
    leg_dir.mkdir()
    entry: dict = {"leg": "leg-4", "ok": False, "reason": "",
                   "receipt_ref": None}
    results: list = []
    pbcanary._run_fanout_leg(
        {}, leg4, leg4.build(), "leg-4", tmp_path, "run-1", None, -10,
        tmp_path, leg_dir, entry, results)
    assert [c["side"] for c in calls] == ["sparky", "sparklina"]
    assert entry["ok"], entry["reason"]
    assert entry["digest_a"] == hashlib.sha256(b"blob-bytes").hexdigest()
    assert entry["digest_b"] == hashlib.sha256(b"blob-bytes").hexdigest()
    row = results[0]
    assert row["digest_a"] == entry["digest_a"]
    assert row["digest_b"] == entry["digest_b"]
    code, _summary = verdict([
        {"leg": "leg-1", "ok": True, "reason": "verified", "receipt_ref": "r1"},
        {"leg": "leg-2", "ok": True, "reason": "verified", "receipt_ref": "r2"},
        {"leg": "leg-3", "ok": True, "reason": "verified", "receipt_ref": "r3"},
        row,
    ])
    assert code == 0


def test_verdict_rejects_mismatched_digest_pair() -> None:
    rows = [
        {"leg": "leg-1", "ok": True, "reason": "verified", "receipt_ref": "r1"},
        {"leg": "leg-2", "ok": True, "reason": "verified", "receipt_ref": "r2"},
        {"leg": "leg-3", "ok": True, "reason": "verified", "receipt_ref": "r3"},
        {"leg": "leg-4", "ok": True, "reason": "verified", "receipt_ref": "r4",
         "digest_a": "a" * 64, "digest_b": "b" * 64},
    ]
    code, summary = verdict(rows)
    assert code == 1
    assert summary["failed_check"] == "envelope-equality"


def test_extra_artifact_digest_key_is_verdict_safe() -> None:
    rows = [
        {"leg": "leg-1", "ok": True, "reason": "v", "receipt_ref": "r1",
         "artifact_digest": "a" * 64},
        {"leg": "leg-2", "ok": True, "reason": "v", "receipt_ref": "r2",
         "artifact_digest": "b" * 64},
        {"leg": "leg-3", "ok": True, "reason": "v", "receipt_ref": "r3",
         "artifact_digest": "c" * 64},
        {"leg": "leg-4", "ok": True, "reason": "v", "receipt_ref": "r4",
         "digest_a": "d" * 64, "digest_b": "d" * 64},
    ]
    assert verdict(rows)[0] == 0


def test_leg12_failure_reasons_avoid_precondition_markers(monkeypatch) -> None:
    monkeypatch.setenv("PBCANARY_GPU_IMAGE", PINNED_IMAGE)
    _, reason = leg1.verify({"artifact": "tampered"}, leg1.build()["expected"])
    _no_markers(reason)
    spec2 = leg2.build()
    _, reason = leg2.verify({"artifact": "IMAGE tampered\n"}, spec2["expected"])
    _no_markers(reason)


def test_summary_dir_writer(tmp_path: Path) -> None:
    rows = [{"leg": "leg-1", "ok": True, "reason": "v", "receipt_ref": "r1"}]
    out = pbcanary.write_summary_dir(tmp_path / "pbcanary-summary",
                                     "run-x", rows, {"exit_code": 0})
    assert (out / "canary-result.json").is_file()
    assert "leg-1: ok" in (out / "summary.txt").read_text()
