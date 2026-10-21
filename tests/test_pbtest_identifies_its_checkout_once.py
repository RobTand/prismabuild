"""One pbtest run identifies its checkout and seals its snapshot once per plan.

On 2026-10-20 TrustLadder's PrismaBuild release gate answered 125 three times:
76 shards across three lanes died with ``pbrun: cannot identify checkout:
... diff-index -p --binary ... timed out after 30 seconds``.  ``pbtest`` ran
one ``pbrun`` per shard, and every ``pbrun`` identified the same checkout
three times (once to seal, twice around its snapshot) and wrote its own
full-history bundle of it.  Sixty shards were 180 concurrent ``git
diff-index -p --binary`` runs over one HDD-backed tree and sixty bundles of
the same commit.

A plan is now prepared once with ``submission.preparation``: one identity,
its two snapshot re-checks and one bundle.  Every shard of the plan is sealed
off that template with its own command, published with
``submission.submit_action`` and observed by ``client_recovery``.  One more
identity read after the last shard is published proves the tree on disk is
still the tree every shard ran; the counts below include it.

The counters wrap the two functions every path goes through --
``core.git_checkout_identity`` and ``submission.write_deterministic_bundle``
-- both in this process and in any ``pbrun`` child, so the same test counts
the old per-shard clients and the new in-process publication alike.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys

from submission_support import patch_store
from test_pbtest import pbtest
from prismabuild import capability_eligibility, contracts, core, pool, submission


ROOT = Path(__file__).resolve().parents[1]
REAL_POPEN = subprocess.Popen

COUNTED = r'''
import os
from prismabuild import core, submission

def _counted(kind, function):
    def counted(*args, **kwargs):
        with open(os.environ["PBT_COUNTS"], "a", encoding="utf-8") as stream:
            stream.write(kind + "\n")
        return function(*args, **kwargs)
    return counted

core.git_checkout_identity = _counted("identity", core.git_checkout_identity)
submission.write_deterministic_bundle = _counted(
    "bundle", submission.write_deterministic_bundle)
'''

#: A per-shard ``pbrun`` client, run as itself against the test's store.
PBRUN_CHILD = r'''
import os, sys
from pathlib import Path
source = Path(os.environ["PBT_SOURCE"])
sys.path.insert(0, str(source / "tools" / "fleet"))
import pbrun
exec(os.environ["PBT_COUNTED"])
from prismabuild.submission_frontend import store_for
configured = store_for(pbrun.__file__, root=Path(os.environ["PBT_ROOT"]))
pbrun.configured_store = lambda: configured
sys.argv = [str(source / "tools" / "fleet" / "pbrun.py"), *sys.argv[1:]]
raise SystemExit(pbrun.main())
'''


def _counted(kind, function):
    """The same counter as ``COUNTED``, for this process."""

    def counted(*args, **kwargs):
        with open(os.environ["PBT_COUNTS"], "a", encoding="utf-8") as stream:
            stream.write(kind + "\n")
        return function(*args, **kwargs)
    return counted


def _checkout(path: Path, files: int) -> Path:
    (path / "tests").mkdir(parents=True)
    for index in range(files):
        (path / "tests" / f"test_{index}.py").write_text(
            f"def test_{index}():\n    pass\n")
    for args in (("init", "-q"), ("config", "user.email", "test@example.invalid"),
                 ("config", "user.name", "PrismaBuild test"), ("add", "-A"),
                 ("commit", "-qm", "suite")):
        done = subprocess.run(["git", "-C", str(path), *args],
                              capture_output=True, text=True, timeout=30)
        assert done.returncode == 0, done.stderr
    return path


def _counted_run(tmp_path: Path, monkeypatch, *, shards: int = 3, wait_s: float = 2,
                 extra: tuple[str, ...] = ()) -> tuple[int, dict[str, int], Path]:
    """Run pbtest over ``shards`` files against a scratch store; count both kinds."""

    root = tmp_path / "fleet"
    counts = tmp_path / "counts"
    counts.touch()
    work = _checkout(tmp_path / "work", shards)
    store = patch_store(monkeypatch, client=pbtest, root=root)
    host = socket.gethostname()
    pool.PoolQueue(store.queue_root).announce(
        host=host, tags=[host, contracts.INTERPRETER_TAG], has_gpu=False,
        capacity={"cpu": 8, "mem_gb": 16}, timeout_ceiling_s=300,
        capabilities=capability_eligibility.OfferCapabilities(
            interpreters=capability_eligibility.InterpreterAnswers.from_observed(
                [sys.executable], None)))
    monkeypatch.setenv("PBT_COUNTS", str(counts))
    monkeypatch.setattr(core, "git_checkout_identity",
                        _counted("identity", core.git_checkout_identity))
    monkeypatch.setattr(submission, "write_deterministic_bundle",
                        _counted("bundle", submission.write_deterministic_bundle))

    def popen(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return REAL_POPEN(command, **kwargs)
        environment = dict(kwargs.pop("env", None) or os.environ)
        environment.update(PBT_SOURCE=str(ROOT), PBT_ROOT=str(root),
                           PBT_COUNTED=COUNTED, PYTHONPATH=str(ROOT / "src"))
        return REAL_POPEN([sys.executable, "-c", PBRUN_CHILD, *command[2:]],
                          env=environment, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    report = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(work), "--python", sys.executable,
        "--shards", str(shards), "--threads-per-shard", "1", "--timeout-s", "120",
        "--wait-s", f"{wait_s:g}", "--tag", host, "--json", str(report), *extra, "tests"])
    code = pbtest.main()
    lines = counts.read_text().split()
    return code, {kind: lines.count(kind) for kind in ("identity", "bundle")}, root


def _sealed(root: Path) -> list[dict]:
    return [json.loads(path.read_text())
            for path in sorted((root / "cas" / "requests").rglob("*.json"))]


def test_a_three_shard_plan_identifies_its_checkout_once_and_bundles_it_once(
        tmp_path, monkeypatch):
    code, counts, root = _counted_run(tmp_path, monkeypatch, shards=3)
    # Three for the one preparation (seal, and the snapshot's two re-checks)
    # and one after the last shard is published.  Per-shard pbrun clients
    # made it nine and three.
    assert counts == {"identity": 4, "bundle": 1}
    actions = _sealed(root)
    assert len(actions) == 3
    assert len({action["action_key"] for action in actions}) == 3
    assert len({json.dumps(action["inputs"][0], sort_keys=True)
                for action in actions}) == 1
    # Nobody claimed them inside --wait-s, so the run is not green; it is
    # still a run, never a refusal.
    assert code == 1


def test_every_shard_of_one_plan_runs_its_own_command_off_one_snapshot(
        tmp_path, monkeypatch):
    _, _, root = _counted_run(tmp_path, monkeypatch, shards=3)
    commands = [action["params"]["command"] for action in _sealed(root)]
    files = sorted(word for command in commands for word in command
                   if word.startswith("tests/test_"))
    assert files == ["tests/test_0.py", "tests/test_1.py", "tests/test_2.py"]
    assert all(sum(word.startswith("tests/test_") for word in command) == 1
               for command in commands)
