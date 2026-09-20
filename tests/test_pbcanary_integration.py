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
import subprocess
import sys
from pathlib import Path

import pytest

FLEET_DIR = Path(__file__).resolve().parents[1] / "tools" / "fleet"
if str(FLEET_DIR) not in sys.path:
    sys.path.insert(0, str(FLEET_DIR))

import pbcanary  # noqa: E402
import pbrun  # noqa: E402
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


# --- the driver's --priority governs every leg (issue #690) ------------------


def test_leg_specs_never_pin_priority_over_the_driver() -> None:
    """Legs 3-4 used to append a pinned --priority after the driver's own.

    On a repeated single-value flag pbrun's argparse lets the later value
    win, so a driver override silently never reached those legs; the
    specs now carry no priority flag at all.
    """

    flags: list[str] = []
    for spec in (leg3.build(), leg4.build()):
        flags += list(spec.get("pbrun_flags", []))
        for action in spec.get("actions", []):
            flags += list(action.get("pbrun_flags", []))
    assert "--priority" not in flags


@pytest.mark.parametrize("pinned", [
    ["--wait-s", "60", "--priority", "-10"],
    ["--wait-s", "60", "--priority=-10"],
])
def test_submit_leg_refuses_a_spec_side_priority(tmp_path: Path, pinned) -> None:
    """A spec pinning --priority is a loud refusal, never a silent override."""

    spec = {"name": "leg-1", "argv": ["true"], "demand": {}}
    with pytest.raises(pbcanary.PreconditionRefused, match="--priority"):
        pbcanary.submit_leg({"pbrun": tmp_path / "pbrun.py"}, spec,
                            tmp_path, "run-1", -10, None,
                            extra_flags=pinned)


def test_submit_leg_puts_the_drivers_priority_on_the_submission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One --priority per submission, and it is the driver's value."""

    captured: dict = {}

    class _Done:
        returncode = 0
        stdout = '{"action_key": "key-1"}'
        stderr = ""

    def fake_run(argv, *, timeout_s):
        captured["argv"] = list(argv)
        return _Done()

    monkeypatch.setattr(pbcanary, "run_process", fake_run)
    spec = {"name": "leg-1", "argv": ["true"], "demand": {"mem_gb": 8}}
    key, detach = pbcanary.submit_leg(
        {"pbrun": tmp_path / "pbrun.py"}, spec, tmp_path, "run-1", -7, None)

    argv = captured["argv"]
    assert argv.count("--priority") == 1
    assert argv[argv.index("--priority") + 1] == "-7"
    assert key == "key-1"
    assert detach["action_key"] == "key-1"


# --- the run namespace records its GC owner (issue #690) ---------------------


def test_run_record_is_a_stamped_gc_contract() -> None:
    record = pbcanary.build_run_record(
        run_id="run-x", generation=None, requested=["leg-1"], priority=-10,
        checkout=Path("/checkout"), published_root=Path("/published"))
    assert record["schema"] == "prismabuild.pbcanary.run.v1"
    assert record["run_id"] == "run-x"
    assert "pb_gc" in record["gc"]["owner"]
    assert "quiescent-store" in record["gc"]["rule"]


# --- the driver's demand reaches pbrun intact (#700) -------------------------


class _PbrunSealed(Exception):
    """The real pbrun reached the CAS seal; the captured demand is complete."""


def _driver_pbrun_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, demand: dict,
) -> list:
    """Submit one fake leg through the driver; return the pbrun argv it built."""

    captured: dict = {}

    class _Done:
        returncode = 0
        stdout = '{"action_key": "key-1"}'
        stderr = ""

    def fake_run(argv, *, timeout_s):
        captured["argv"] = list(argv)
        return _Done()

    monkeypatch.setattr(pbcanary, "run_process", fake_run)
    spec = {"name": "leg-2", "argv": ["true"], "demand": demand}
    pbcanary.submit_leg(
        {"pbrun": tmp_path / "pbrun.py"}, spec, tmp_path, "run-1", -7, None)
    return captured["argv"]


def _submit_leg_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, spec: dict,
) -> list:
    """Submit one spec through the driver; return the pbrun argv it built."""

    captured: dict = {}

    class _Done:
        returncode = 0
        stdout = '{"action_key": "key-1"}'
        stderr = ""

    def fake_run(argv, *, timeout_s):
        captured["argv"] = list(argv)
        return _Done()

    monkeypatch.setattr(pbcanary, "run_process", fake_run)
    pbcanary.submit_leg(
        {"pbrun": tmp_path / "pbrun.py"}, spec, tmp_path, "run-1", -7, None)
    return captured["argv"]


def _sealed_pbrun_params(
    pbrun_argv: list, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> dict:
    """Decode one driver command line through pbrun's real parser and defaults.

    The same seam ``tests/test_pbrun_declares_cores.py`` uses: ``pbrun.main``
    parses the command line, resolves the default environment and the demand,
    and is stopped at ``pb.seal_action`` -- no queue, worker or fleet contact.
    The checkout the driver named is a private Git repository here.
    """

    sealed: list = []
    assert subprocess.run(
        ["git", "init", "-q", str(tmp_path)], check=False
    ).returncode == 0
    assert subprocess.run(
        ["git", "-C", str(tmp_path),
         "-c", "user.name=PrismaBuild test",
         "-c", "user.email=test@example.invalid",
         "commit", "--allow-empty", "-qm", "fixture"],
        check=False,
    ).returncode == 0

    def _stop(body, *_a, **_kw):
        sealed.append(body)
        raise _PbrunSealed()

    monkeypatch.setattr(pbrun.pb, "seal_action", _stop)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(pbrun, "git_repository_root", lambda _cwd: tmp_path)
    monkeypatch.setattr(
        pbrun, "build_git_checkout_snapshot",
        lambda *_a, **_kw: {"input": {"id": "test"}},
    )
    monkeypatch.setattr(sys, "argv", ["pbrun.py", *pbrun_argv[2:]])
    with pytest.raises(_PbrunSealed):
        pbrun.main()
    return sealed[0]["params"]


def _sealed_pbrun_demand(
    pbrun_argv: list, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> dict:
    return _sealed_pbrun_params(pbrun_argv, tmp_path, monkeypatch)["demand"]


def test_submit_leg_demand_survives_the_real_pbrun_parser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression (#700): repeated scalar ``--demand`` options lose all but the last.

    ``--demand`` is one single-value flag, so ``--demand cpu=3 --demand gpu=1
    --demand mem_gb=8`` seals only ``mem_gb=8``.  The live leg-2 action
    ``f385165a...`` sealed ``{"cpu": 1, "mem_gb": 8}`` that way: the declared
    ``gpu=1`` admission was lost while the payload still ran CUDA.  The driver
    must send the one comma-separated aggregate pbrun already parses, with
    every resource preserved, including a nondefault cpu count.
    """

    argv = _driver_pbrun_argv(
        tmp_path, monkeypatch, {"cpu": 3, "gpu": 1, "mem_gb": 8})
    assert _sealed_pbrun_demand(argv, tmp_path, monkeypatch) == {
        "cpu": 3, "gpu": 1, "mem_gb": 8}, argv


def test_submit_leg_empty_demand_keeps_the_pbrun_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty demand is no flag at all, so pbrun's own defaults stand."""

    argv = _driver_pbrun_argv(tmp_path, monkeypatch, {})
    assert "--demand" not in argv
    assert _sealed_pbrun_demand(argv, tmp_path, monkeypatch) == {
        "cpu": 1, "mem_gb": 4}


# --- the leg's pinned image becomes a sealed claim requirement (#714) --------

def test_submit_leg_declares_a_present_container_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A spec carrying ``container_image`` is submitted with the flag.

    Leg 2's spec has always named the digest-pinned campaign image for its
    metadata and its ``docker run``; the driver never forwarded it, so the
    action was admitted on tags alone and a box without the image could
    claim it and fail inside the payload -- #714 replayed by the canary
    itself.
    """

    spec = {"name": "leg-2", "argv": ["true"], "demand": {"gpu": 1},
            "container_image": PINNED_IMAGE}
    argv = _submit_leg_argv(tmp_path, monkeypatch, spec)
    assert argv.count("--container-image") == 1
    assert argv[argv.index("--container-image") + 1] == PINNED_IMAGE
    # An option of pbrun's, before the command separator.
    assert argv.index("--container-image") < argv.index("--")


@pytest.mark.parametrize("value", [None, "", "   "])
def test_submit_leg_omits_the_flag_when_there_is_no_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value,
) -> None:
    """Ordinary legs keep their command line: no empty declaration."""

    spec = {"name": "leg-1", "argv": ["true"], "demand": {}}
    if value is not None:
        spec["container_image"] = value
    argv = _submit_leg_argv(tmp_path, monkeypatch, spec)
    assert "--container-image" not in argv


def test_submit_leg_container_image_reaches_the_sealed_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end through pbrun's real parser: the image is a sealed
    ``params.container_images`` requirement, which is what makes a box
    without the image deny the claim instead of spending the attempt.
    """

    spec = {"name": "leg-2", "argv": ["true"], "demand": {"gpu": 1},
            "container_image": PINNED_IMAGE}
    argv = _submit_leg_argv(tmp_path, monkeypatch, spec)
    params = _sealed_pbrun_params(argv, tmp_path, monkeypatch)
    assert params["container_images"] == [PINNED_IMAGE]
