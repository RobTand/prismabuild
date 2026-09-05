"""The store's reaper removes only what no execution can still reach.

Nothing in PrismaBuild has ever removed a claim, a ``.worker-locks`` entry, a
staging namespace. They are immutable by design and
minted per execution, so a campaign leaves one of each behind every time it
runs an action in a fresh materialized checkout. The live store held 1785
claims, 1745 locks and 942 empty namespaces when this was written.

The whole question is which of those a live execution still needs, and the
fixtures here answer it the only way that proves anything: every litter class
is minted by the code that mints it in production. The claims, locks and
namespaces come out of real ``core.run_local_action`` calls and the
in-flight lock out of ``core._local_output_lock``. A fixture that wrote these files by hand, with
this test's own idea of the lock's digest or the namespace's name, would prove
that ``pb_gc`` agrees with the test rather than with the store.

The tool runs as a subprocess wherever liveness is the subject. Both live
guards are answers about *other* processes, ``flock`` on an open file
description and an inode held open in ``/proc``, and an in-process call would
let the test's own descriptors stand in for a worker's.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402

import pb_gc  # noqa: E402

TOOL = REPOSITORY / "tools" / "fleet" / "pb_gc.py"


# --------------------------------------------------------------------------
# Fixtures: a store whose litter was minted by the code that mints it
# --------------------------------------------------------------------------


def _action(checkout: Path, *, tag: str) -> dict[str, object]:
    """One sealed action that writes ``result.bin`` into ``checkout``.

    ``tag`` reaches ``params``, so each execution is a distinct action key. It
    has to be: a second run of one key is a cache hit that returns before any
    claim is published, which is exactly why the live store holds fewer claims
    than it has run actions.
    """

    (checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")
    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/pb-gc",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": [
                sys.executable, "-c",
                "import pathlib; pathlib.Path('result.bin').write_bytes(b'r')",
            ],
            "working_directory": ".",
            "result_path": "result.bin",
        },
        "inputs": [{"id": "model/config", "sha256": "2" * 64, "bytes": 20}],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"tag": tag},
        "environment": {"variables": {"DECLARED": "yes"}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable",
            "platform_key": None,
            "host_class": None,
        },
    })


def _execute(tmp_path: Path, tag: str) -> dict[str, object]:
    """Run one action in its own checkout root, as a materialized job does.

    ``materialize._execution_checkout`` mints a fresh ``mkdtemp`` root for
    every execution, so a per-execution root is what this imitates. Returns
    the paths of the three droppings the run leaves in the store.
    """

    cas_root = tmp_path / "cas"
    checkout = tmp_path / "checkouts" / tag
    checkout.mkdir(parents=True)
    action = _action(checkout, tag=tag)
    result = pb.run_local_action(
        action, cas_root=cas_root, checkout_root=checkout)
    assert result["status"] == "published"
    digest = str(result["local_result_claim_sha256"])
    lock = pb_gc.output_lock_name({
        "checkout_root": str(checkout.resolve()),
        "working_directory": ".",
        "result_path": "result.bin",
    })
    return {
        "checkout": checkout,
        "action": action,
        "claim": cas_root / "local-results" / "v1" / digest[:2] / f"{digest}.json",
        "lock": cas_root / ".worker-locks" / lock,
        "namespace": cas_root / ".staging" / "local-results" / digest,
    }


def _droppings(execution: dict[str, object]) -> list[Path]:
    return [Path(str(execution[name])) for name in ("claim", "lock", "namespace")]


def _run(*argv: str, expect: int = 0) -> str:
    completed = subprocess.run(
        [sys.executable, str(TOOL), *argv],
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == expect, completed.stderr
    return completed.stdout


def _snapshot(root: Path) -> dict[str, object]:
    """Every path under ``root`` with its mode, size and content digest."""

    found: dict[str, object] = {}
    for directory, subdirectories, files in os.walk(root):
        base = Path(directory)
        found[str(base.relative_to(root))] = "dir"
        for name in (*subdirectories, *files):
            path = base / name
            relative = str(path.relative_to(root))
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                found[relative] = ("dir", stat.S_IMODE(info.st_mode))
            elif stat.S_ISLNK(info.st_mode):
                found[relative] = ("link", os.readlink(path))
            else:
                found[relative] = (
                    "file",
                    stat.S_IMODE(info.st_mode),
                    info.st_size,
                    hashlib.sha256(path.read_bytes()).hexdigest(),
                )
    return found


# --------------------------------------------------------------------------
# The structural rule
# --------------------------------------------------------------------------


def test_the_lock_name_this_tool_derives_is_the_one_core_actually_creates(
    tmp_path: Path,
):
    """The reaper's idea of a claim's lock must be the store's idea of it.

    Nothing in a ``.worker-locks`` file says which action owns it: the name is
    a digest of the output path and the file is empty. So the only way to keep
    a live claim's lock is to re-derive the name, and the only way to know the
    derivation is right is to take the real lock and look at what appeared.
    """

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    checkout = tmp_path / "checkout"
    (checkout / "sub").mkdir(parents=True)
    output = checkout / "sub" / "result.bin"
    with pb._local_output_lock(cas, checkout, output):
        created = [p.name for p in (cas.root / ".worker-locks").iterdir()]
    assert created == [pb_gc.output_lock_name({
        "checkout_root": str(checkout.resolve()),
        "working_directory": "sub",
        "result_path": "result.bin",
    })]


def test_an_abandoned_execution_is_swept_and_a_live_one_is_untouched(
    tmp_path: Path,
):
    """The headline property, with the age backstop switched off.

    Both executions are real and identical in every respect except one: the
    abandoned one's checkout root has been removed, exactly as
    ``materialize._cleanup_execution_checkout`` removes it in its ``finally``.
    That single structural difference is what decides all six files, which is
    the point of running at ``--min-age-hours 0``: nothing here is decided by
    how old anything is.
    """

    live = _execute(tmp_path, "live")
    dead = _execute(tmp_path, "dead")
    shutil.rmtree(Path(str(dead["checkout"])))

    _run("--cas-root", str(tmp_path / "cas"), "--apply", "--min-age-hours", "0")

    for path in _droppings(live):
        assert path.exists(), f"a live execution lost {path}"
    for path in _droppings(dead):
        assert not path.exists(), f"an abandoned execution kept {path}"


def test_a_dry_run_reports_the_same_entries_and_writes_nothing(tmp_path: Path):
    """``--dry-run`` is the default, and it must leave the store byte-identical.

    Byte-identical rather than "the litter is still there": the tool opens
    lock files to probe their ``flock`` and scans directories it might later
    remove, and any of that creating a file, a directory or a byte would be a
    reporting tool mutating the thing it reports on.
    """

    _execute(tmp_path, "live")
    dead = _execute(tmp_path, "dead")
    shutil.rmtree(Path(str(dead["checkout"])))
    cas_root = tmp_path / "cas"

    before = _snapshot(cas_root)
    screen = _run("--cas-root", str(cas_root), "--min-age-hours", "0")
    assert _snapshot(cas_root) == before

    assert "nothing removed; re-run with --apply" in screen
    for path in _droppings(dead):
        assert str(path) in screen
    assert "total: 3 entries" in screen


def test_the_age_backstop_keeps_an_abandoned_execution_that_is_still_fresh(
    tmp_path: Path,
):
    """A structural verdict plus a backstop, in that order.

    The checkout root is gone, so the structural rule condemns all three
    files. The backstop keeps them anyway, because a root that is absent here
    can be a live execution on another box: the roots are box-local and
    ``materialize.LOCAL_CHECKOUT_ROOT`` spells the same path on every one.
    """

    dead = _execute(tmp_path, "dead")
    shutil.rmtree(Path(str(dead["checkout"])))

    screen = _run(
        "--cas-root", str(tmp_path / "cas"), "--apply", "--min-age-hours", "24")

    assert "removed 0 entries" in screen
    assert "younger than the age backstop" in screen
    for path in _droppings(dead):
        assert path.exists()


# --------------------------------------------------------------------------
# Liveness: the two guards that answer about other processes
# --------------------------------------------------------------------------


def test_a_lock_taken_before_its_claim_exists_survives_apply(tmp_path: Path):
    """The window ``run_local_action`` really has, and the guard that covers it.

    ``run_local_action`` enters ``_local_output_lock`` and only then publishes
    the claim, so an execution between those two points owns a lock that no
    claim protects. Nothing but the lock itself says it is live, and this is
    the test that the tool asks it.
    """

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    output = checkout / "result.bin"
    with pb._local_output_lock(cas, checkout, output):
        lock = next((cas.root / ".worker-locks").iterdir())
        screen = _run(
            "--cas-root", str(cas.root), "--apply", "--min-age-hours", "0")
        assert lock.exists()
    assert "held by a live local action" in screen
    assert "removed 0 entries" in screen


def test_a_worker_lock_belonging_to_a_live_claim_survives_apply(tmp_path: Path):
    """A finished execution whose checkout is still there keeps its lock.

    Nothing holds this lock: the run ended and released it. It survives on the
    claim alone, which is the guard that has to work for the persistent
    checkout root a path-addressed action runs in.
    """

    live = _execute(tmp_path, "live")
    lock = Path(str(live["lock"]))
    assert pb_gc._probe_lock(lock) == "", "the fixture's lock is still held"

    screen = _run(
        "--cas-root", str(tmp_path / "cas"), "--apply", "--min-age-hours", "0")

    assert lock.exists()
    assert "output path of a live local result claim" in screen


# --------------------------------------------------------------------------
# What is a record and not litter
# --------------------------------------------------------------------------


def test_requests_and_receipts_are_reported_and_never_removed(tmp_path: Path):
    """The 1324-versus-934 gap is a diagnostic, not a work list."""

    dead = _execute(tmp_path, "dead")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    request = cas.publish_action_request(dead["action"])
    receipt = cas._receipt_path(str(dead["action"]["action_key"]))
    shutil.rmtree(Path(str(dead["checkout"])))
    assert request.exists() and receipt.exists()

    screen = _run(
        "--cas-root", str(cas.root), "--apply", "--min-age-hours", "0")

    assert request.exists(), "a request was removed"
    assert receipt.exists(), "a receipt was removed"
    assert "1 requests, 1 receipts, 0 requests with no receipt" in screen


def test_a_staging_namespace_that_still_holds_a_payload_is_left_for_repair(
    tmp_path: Path,
):
    """A killed publication is not this tool's to unwind.

    ``core.repair_local_result`` removes a staged payload under the output
    lock, after proving the file is regular and this user's. This tool holds
    no such lock and makes no such proof, so it reports the directory and
    leaves it.
    """

    dead = _execute(tmp_path, "dead")
    namespace = Path(str(dead["namespace"]))
    payload = namespace / ".payload.killed.tmp"
    payload.write_bytes(b"half a result")
    shutil.rmtree(Path(str(dead["checkout"])))

    screen = _run(
        "--cas-root", str(tmp_path / "cas"), "--apply", "--min-age-hours", "0")

    assert payload.exists()
    assert namespace.exists()
    assert "clear it with core.repair_local_result" in screen
    assert "1 staging namespaces still hold a payload" in screen


def test_an_unexpected_entry_is_reported_and_never_removed(tmp_path: Path):
    """Anything the sweeper does not recognize is somebody else's file."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    for parts in (
        (".worker-locks", "notes.txt"),
        (".staging", "leftover.bin"),
        (".staging", "local-results", "README"),
        ("local-results", "v1", "ab", "notes.txt"),
    ):
        path = cas.root.joinpath(*parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"not litter")

    screen = _run(
        "--cas-root", str(cas.root), "--apply", "--min-age-hours", "0")

    for parts in (
        (".worker-locks", "notes.txt"),
        (".staging", "leftover.bin"),
        (".staging", "local-results", "README"),
        ("local-results", "v1", "ab", "notes.txt"),
    ):
        assert cas.root.joinpath(*parts).exists()
    assert "unexpected entry" in screen
    assert "removed 0 entries" in screen


# --------------------------------------------------------------------------
# The command line itself
# --------------------------------------------------------------------------


def test_removing_requires_apply_and_a_root_must_be_typed(tmp_path: Path):
    """No default root, because the default would be the live fleet store."""

    parser = pb_gc.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    assert parser.parse_args(["--cas-root", str(tmp_path)]).apply is False


def test_a_root_that_is_not_a_directory_is_refused_rather_than_swept(
    tmp_path: Path,
):
    completed = subprocess.run(
        [sys.executable, str(TOOL), "--cas-root", str(tmp_path / "absent")],
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 2
    assert "not a CAS root" in completed.stderr


def test_the_survey_never_creates_a_directory_the_store_does_not_have(
    tmp_path: Path,
):
    """An empty store must survey clean, and stay empty.

    ``core._copy_to_staging`` and ``core._local_output_lock`` both create their
    directory on use. A sweeper that reused that habit would mint ``.staging``
    and ``.worker-locks`` in every store it looked at.
    """

    cas_root = tmp_path / "cas"
    cas_root.mkdir()
    plan = pb_gc.survey(cas_root, min_age_s=0.0)
    assert plan["remove"] == []
    assert list(os.listdir(cas_root)) == []


def test_a_claim_whose_checkout_root_cannot_be_inspected_is_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Undecidable is not absent.

    An NFS root that answers ``EACCES``, or a stale handle, says nothing about
    whether an execution is live. The claim stays, and so does everything it
    protects.
    """

    dead = _execute(tmp_path, "dead")
    root = str(Path(str(dead["checkout"])).resolve())
    shutil.rmtree(Path(str(dead["checkout"])))
    real_lstat = os.lstat

    def refusing(path, *args, **kwargs):
        if str(path) == root:
            raise PermissionError(13, "Permission denied")
        return real_lstat(path, *args, **kwargs)

    monkeypatch.setattr(pb_gc.os, "lstat", refusing)
    plan = pb_gc.survey(tmp_path / "cas", min_age_s=0.0)
    assert plan["remove"] == []
    reasons = {str(row["why"]) for row in plan["keep"]}
    assert "checkout root could not be inspected" in reasons
    assert "output path of a live local result claim" in reasons
