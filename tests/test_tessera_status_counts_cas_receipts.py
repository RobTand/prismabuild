"""The export status screen, counting the export rather than a directory.

A shard's manifest is the action's declared result. Under SLURM the action
runs in a private checkout the job removes when it ends, so the manifest lives
where it always lived, in the CAS as the receipt's verified result, and never
appears under the shared checkout. The screen globbed the shared directory, so
a successful current export read as ``0/120 encoded`` while a manifest left
behind by a previous plan could still decide the count.

The subject here is the reader. One shard is encoded through the real producer,
the real snapshot sealer and materializer, and the real local execution, with a
CPU-only wrapper standing in for the external Tessera installation.
"""
from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import materialize  # noqa: E402

import dispatch_tessera_shards as shards  # noqa: E402
import fleet_submit  # noqa: E402
import tessera_status  # noqa: E402

#: The stand-in exporter: writes one schema-compatible manifest and exits.
_WRAPPER = (
    "import json, pathlib, sys\n"
    "shard = int(sys.argv[sys.argv.index('--shard') + 1])\n"
    "out = pathlib.Path(sys.argv[sys.argv.index('--result') + 1])\n"
    "out.parent.mkdir(parents=True, exist_ok=True)\n"
    "out.write_text(json.dumps({'shard': shard, 'total_bytes': 16,\n"
    "                           'quantized_bytes': 8,\n"
    "                           'quantized_params': 32}))\n"
)


def _checkout(root: Path) -> Path:
    root.mkdir()
    for argv in (
        ["init", "-q"],
        ["config", "user.name", "PrismaBuild test"],
        ["config", "user.email", "t@example.invalid"],
    ):
        subprocess.run(["git", "-C", str(root), *argv], check=True)
    (root / shards.WRAPPER).write_text(_WRAPPER, encoding="utf-8")
    encoder = root / "tessera" / "src" / "audit_encoder.py"
    encoder.parent.mkdir(parents=True)
    encoder.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "commit", "-qm", "staged encoder"], check=True)
    return root


def _dispatcher(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                *, plan: str = '{"rung": 896}\n') -> tuple[Path, Path, str]:
    """The shard dispatcher pointed at this test's own tree, plus its plan."""

    checkout = _checkout(tmp_path / "shared-checkout")
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(plan, encoding="utf-8")
    monkeypatch.setattr(shards, "CHECKOUT", checkout)
    monkeypatch.setattr(shards, "PYTHON", sys.executable)
    monkeypatch.setattr(shards, "SOURCE", str(tmp_path / "unused-model"))
    monkeypatch.setattr(shards, "PLAN", str(plan_path))
    monkeypatch.setattr(shards, "PARTS", str(tmp_path / "unused-parts"))
    return checkout, plan_path, shards.sha256_file(plan_path)


def _encode_one_shard(tmp_path: Path, checkout: Path, plan_sha: str,
                      cas: pb.PrismaBuildCAS, shard: int = 1) -> dict:
    """One shard, all the way to a published receipt, through the SLURM path."""

    closure = pb.build_code_closure(checkout, shards.closure_files())
    action = shards.build_action(shard, closure, plan_sha)
    fleet_submit._SNAPSHOT_CACHE.clear()
    action, snapshot = fleet_submit.seal_checkout_into_action(
        action, cas=cas, checkout_root=checkout)
    cas.publish_action_request(action)
    item = {
        "action_key": action["action_key"],
        "cas_root": str(cas.root),
        "checkout_snapshot": snapshot,
    }
    with materialize._execution_checkout(
        item, local_checkout_root=tmp_path / "local"
    ) as private:
        result = pb.run_local_action(
            action, cas_root=cas.root, checkout_root=private)
        assert (private / action["task"]["result_path"]).is_file()
    # The private checkout is gone and the manifest went with it. The receipt
    # is what is left, and it is the whole record of the work.
    assert not private.exists()
    assert result["status"] == "published"
    assert cas.lookup(action) is not None
    return action


def _screen(tmp_path: Path, cas: pb.PrismaBuildCAS, results: Path,
            plan_path: Path, monkeypatch: pytest.MonkeyPatch,
            transport: str = "slurm", *, flags: bool = False,
            expect: int = tessera_status.EXIT_OK) -> str:
    """The command, reading this test's store instead of the fleet's.

    The store root and the plan travel as module constants rather than as
    command-line flags, so the same invocation runs against either version of
    the reader and the assertion is about the count rather than about which
    options exist.

    The queue and CAS roots are created because these tests mean an empty
    store rather than an absent one, and the screen tells those apart: a root
    that is not there is a root it could not read, and it says so.
    """

    (tmp_path / "queue").mkdir(exist_ok=True)
    cas.root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        tessera_status, "describe_squeue_depth",
        lambda **_kwargs: "no jobs queued or running")
    monkeypatch.setattr(tessera_status, "CAS", cas.root, raising=False)
    argv = [
        "--transport", transport,
        "--queue-root", str(tmp_path / "queue"),
        "--results-root", str(results),
    ]
    if flags:
        argv += ["--cas-root", str(cas.root), "--plan", str(plan_path)]
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        code = tessera_status.main(argv)
    assert code == expect
    return output.getvalue()


def test_a_slurm_shard_with_no_shared_manifest_counts_as_encoded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The defect: a verified receipt for shard 1, and a screen reading 0/120."""

    checkout, plan_path, plan_sha = _dispatcher(tmp_path, monkeypatch)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    _encode_one_shard(tmp_path, checkout, plan_sha, cas)
    results = checkout / "results" / "glm53-tessera"
    assert not results.exists()

    printed = _screen(tmp_path, cas, results, plan_path, monkeypatch)

    assert "shards     1/120 encoded   missing 119" in printed
    assert "1 from CAS receipts" in printed
    # And its bytes reach the totals, which read zero for the same reason.
    assert "over 32 params" in printed


def test_a_receipt_from_a_previous_plan_is_not_this_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half: a stale result must not decide the screen."""

    checkout, plan_path, plan_sha = _dispatcher(tmp_path, monkeypatch)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    _encode_one_shard(tmp_path, checkout, plan_sha, cas)

    # The allocation is redone. Every shard of the new plan is unencoded, and
    # the old receipts are another export's however verifiable they are.
    plan_path.write_text('{"rung": 512}\n', encoding="utf-8")
    printed = _screen(
        tmp_path, cas, checkout / "results" / "glm53-tessera", plan_path,
        monkeypatch)

    assert "shards     0/120 encoded   missing 120" in printed
    assert "stale    receipts encoding a different plan: 1" in printed


def test_the_pull_queues_own_manifests_still_count_under_the_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The old transport wrote files into the checkout, and they are its record."""

    checkout, plan_path, _plan_sha = _dispatcher(tmp_path, monkeypatch)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    results = checkout / "results" / "glm53-tessera"
    results.mkdir(parents=True)
    (results / "shard-00002.json").write_text(json.dumps({
        "shard": 2, "total_bytes": 4, "quantized_bytes": 2,
        "quantized_params": 8,
    }), encoding="utf-8")

    printed = _screen(
        tmp_path, cas, results, plan_path, monkeypatch, transport="pool")

    assert "shards     1/120 encoded   missing 119" in printed
    assert "0 from CAS receipts, 1 from" in printed


def test_a_status_screen_survives_an_unreadable_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A screen that crashes is worse than a screen that says what it read."""

    checkout, plan_path, plan_sha = _dispatcher(tmp_path, monkeypatch)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _encode_one_shard(tmp_path, checkout, plan_sha, cas)
    key = str(action["action_key"])
    request = cas.root / "requests" / key[:2] / f"{key}.json"
    # Truncated after the action kind, so the entry reaches the parser rather
    # than being passed over as some other action's request.
    request.chmod(0o600)
    request.write_text(
        '{"task": {"definition_id": "tessera/glm53-export-shard"',
        encoding="utf-8")

    printed = _screen(
        tmp_path, cas, checkout / "results" / "glm53-tessera", plan_path,
        monkeypatch, expect=tessera_status.EXIT_PARTIAL)

    assert "shards     0/120 encoded   missing 120" in printed
    assert "skipped  entries that could not be read: 1" in printed
    assert str(request) in printed


def test_an_unreadable_plan_is_reported_rather_than_guessed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No plan digest means no export identity, and the screen says so."""

    checkout, plan_path, plan_sha = _dispatcher(tmp_path, monkeypatch)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    _encode_one_shard(tmp_path, checkout, plan_sha, cas)
    plan_path.unlink()

    printed = _screen(
        tmp_path, cas, checkout / "results" / "glm53-tessera", plan_path,
        monkeypatch, expect=tessera_status.EXIT_PARTIAL)

    assert "shards     0/120 encoded   missing 120" in printed
    assert "plan     unreadable" in printed


def test_the_store_and_the_plan_can_be_named_on_the_command_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two hooks the tests and an operator with a second store need."""

    checkout, plan_path, plan_sha = _dispatcher(tmp_path, monkeypatch)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    _encode_one_shard(tmp_path, checkout, plan_sha, cas)
    monkeypatch.setattr(tessera_status, "CAS", tmp_path / "no-such-cas")

    printed = _screen(
        tmp_path, cas, checkout / "results" / "glm53-tessera", plan_path,
        monkeypatch, flags=True)

    assert "shards     1/120 encoded   missing 119" in printed
